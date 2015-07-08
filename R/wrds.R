# You can learn more about package authoring with RStudio at:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Build and Reload Package:  'Ctrl + Shift + B'
#   Check Package:             'Ctrl + Shift + E'
#   Test Package:              'Ctrl + Shift + T'
#
# Other documentation regarding OOP in R:
# ReferenceClasses: http://www.inside-r.org/r-doc/methods/ReferenceClasses
# StackOverflow: http://stackoverflow.com/questions/9521651/r-and-object-oriented-programming

.onLoad <- function(libname, pkgname) {
    .jpackage(pkgname, lib.loc=libname)
}

#' Create a client connection to WRDS
#'
#' @param username Username. Don't pass to use stored username.
#' @param password Password. Don't pass to use stored password.
#' @return A client connection object
#' @examples
#' cl <- wrdsClient("wrdsUser", "xyz123")
#' cl <- wrdsClient()
wrdsClient <- function(username = NULL, password = NULL) {
    sasPath <- getOption("wrds.sasPath")

    if (is.null(sasPath)) {
        stop("No SAS path set. Please set one with wrdsSASPath() and give it the full path to the SAS JAR installation.")
    }

    sasCore <- paste0(sasPath, "/sas.core.jar")
    sasDriver <- paste0(sasPath, "/sas.intrnet.javatools.jar")
    if (!file.exists(sasDriver)) {
        stop(paste("SAS driver", sasDriver, "not found!"))
    }

    .jaddClassPath(c(sasCore, sasDriver))

    driver <- RJDBC::JDBC("com.sas.net.sharenet.ShareNetDriver", sasDriver, identifier.quote="`")

    username <- ifelse(is.null(username), getOption("wrds.username"), username)
    password <- ifelse(is.null(password), getOption("wrds.password"), password)

    client <- RJDBC::dbConnect(driver, "jdbc:sharenet://wrds-cloud.wharton.upenn.edu:8551/", username, password)

    client
}

#' Store your WRDS account credentials
#' These will be automatically used when calling wrdsClient without parameters
#' Note: this function does NOT check your credentials!
#' @param username Your username
#' @param password Your password
#' @examples
#' wrdsCredentials("wrdsUser", "xyz123")
wrdsCredentials <- function(username, password) {
    options(wrds.username = username)
    options(wrds.password = password)
}

#' Set the SAS drivers path
#' This is the path that contains sas.core.jar and sas.intrnet.javatools.jar.
#' The WRDS package needs them to be able to connect to the WRDS service
#' @param path The path to the directory containing the necessary JAR files
#' @examples
#' wrdsSASPath("/usr/local/SAS")
wrdsSASPath <- function(path) {
    if (!dir.exists(path)) {
        stop(paste("Could not find directory", path))
    }

    options(wrds.sasPath = path)
}

# wrdsUpdateLink <- function(filename) {
#     # Reading in the given results file
#     linkdata = read.csv(filename, sep="\t", quote="")
#
#     wrdsdb = .wrdsGetDb()
#
#     # Splitting data in a master part, and the link information part
#     master.data = subset(linkdata, select=-c(LINKPRIM, LIID, LINKTYPE, LPERMNO, LPERMCO, LINKDT, LINKENDDT))
#     link.data = subset(linkdata, select=c(gvkey, LINKPRIM, LIID, LINKTYPE, LPERMNO, LPERMCO, LINKDT, LINKENDDT))
#
#     # Making the data in the master.data unique
#     master.data = unique(master.data)
#     dbWriteTable(wrdsdb, "master", master.data, overwrite=TRUE)
#
#     # Convert dates to YYYY-MM-DD. SQLite via R does not have any native datetype fields, so as long
#     # as they are kept in YYYY-MM-DD format, comparisons will work fine.
#     link.data[link.data$LINKENDDT == "E", ]$LINKENDDT = NA
#     link.data$LINKDT = as.character(as.Date(as.character(link.data$LINKDT), format="%Y%m%d"))
#     link.data$LINKENDDT = as.character(as.Date(as.character(link.data$LINKENDDT), format="%Y%m%d"))
#     dbWriteTable(wrdsdb, "link", link.data, overwrite=TRUE)
#
#     # Maybe update the field properties (primary key, indexing, etc) for faster access
# }

#' Updates the base tables in the SQLite database
#' It runs SQL commands on the WRDS cloud, fetches the data, and puts that data in their
#' respective SQLite tables.
#' @param conn The WRDS cloud connection obtained with wrdsClient()
#' @examples
#' cl <- wrdsClient()
#' wrdsUpdateTables(cl)
wrdsUpdateTables <- function(conn) {
    localdb = .wrdsGetDb()

    mapper <- list(
        master = "COMPMASTER",
        head = "COMPHEAD",
#        hist = "COMPHIST",
        link = "CCMXPF_LNKHIST"
    )

    batchSize <- 1e5

    df <- data.frame(mapper)
    cols <- colnames(df)
    for(i in 1:length(cols)) {
        if (DBI::dbExistsTable(localdb, cols[i])) DBI::dbRemoveTable(localdb, cols[i])

        j = 1;
        message(paste("Querying WRDS for", cols[i], "data"))
        res <- DBI::dbSendQuery(conn, paste0("SELECT * FROM CRSP.", df[1, i]))
        while(TRUE) {
            message(paste("...", as.integer(j * batchSize)))
            data <- fetch(res, n=batchSize)
            if (nrow(data) == 0) break;

            DBI::dbWriteTable(localdb, cols[i], data, append=TRUE)
            j = j + 1
        }

        DBI::dbClearResult(res)
    }
}
