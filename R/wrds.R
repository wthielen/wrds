# You can learn more about package authoring with RStudio at:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Build and Reload Package:  'Ctrl + Shift + B'
#   Check Package:             'Ctrl + Shift + E'
#   Test Package:              'Ctrl + Shift + T'

#' Create a client connection to WRDS
#'
#' @param username Username. Don't pass to use stored username.
#' @param password Password. Don't pass to use stored password.
#' @return A client connection object
#' @examples
#' cl <- wrdsClient("wrdsUser", "xyz123")
#' cl <- wrdsClient()
wrdsClient <- function(username = NULL, password = NULL) {
    if (!('package:RJDBC' %in% search() || require('RJDBC', quiet=TRUE))) {
        stop("Package RJDBC could not be loaded")
    }

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

#' Updates the master and link tables in the SQLite database
#' Pass it the filename of the results file, and it will split the data and import them
#' into SQLite.
#' The filename is expected to be in tab-separated format.
#' Maybe other formats can be detected and added later on.
#' @param filename The name of the results file from WRDS cloud
#' @examples
#' wrdsUpdateLink("Gvkey2Permno.txt")
wrdsUpdateLink <- function(filename) {
    # Reading in the given results file
    linkdata = read.csv(filename, sep="\t", quote="")

    wrdsdb = .wrdsGetDb()

    # Splitting data in a master part, and the link information part
    master.data = subset(linkdata, select=-c(LINKPRIM, LIID, LINKTYPE, LPERMNO, LPERMCO, LINKDT, LINKENDDT))
    link.data = subset(linkdata, select=c(gvkey, LINKPRIM, LIID, LINKTYPE, LPERMNO, LPERMCO, LINKDT, LINKENDDT))

    # Making the data in the master.data unique
    master.data = unique(master.data)
    dbWriteTable(wrdsdb, "master", master.data, overwrite=TRUE)

    # Convert dates to YYYY-MM-DD. SQLite via R does not have any native datetype fields, so as long
    # as they are kept in YYYY-MM-DD format, comparisons will work fine.
    link.data[link.data$LINKENDDT == "E", ]$LINKENDDT = NA
    link.data$LINKDT = as.character(as.Date(as.character(link.data$LINKDT), format="%Y%m%d"))
    link.data$LINKENDDT = as.character(as.Date(as.character(link.data$LINKENDDT), format="%Y%m%d"))
    dbWriteTable(wrdsdb, "link", link.data, overwrite=TRUE)

    # Maybe update the field properties (primary key, indexing, etc) for faster access
}
