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

#' Synchronizes the base tables in the SQLite database
#' This is going to be the basic function to initialize the database for
#' the common WRDS working environment
#' TODO Needs discussion what to include
#' @param conn The WRDS cloud connection obtained with wrdsClient()
#' @examples
#' cl <- wrdsClient()
#' wrdsSyncTables(cl)
wrdsSyncTables <- function(conn) {
    localdb = .wrdsGetDb()

    ## Download main tables
    tables <- c(
        "COMPMASTER", "COMPHEAD", "MSP500LIST", "MSP500", "CCMXPF_LNKHIST"
    )
    wrdsUpdateTable(conn, tables)


    ## Get SP500 GVKEYs and use them to update the COMPHIST table
    gvkeys <- wrdsGetSP500Gvkeys()
    wrdsUpdateTable(conn, "COMPHIST", gvkeys=gvkeys)
}

#' Updates the given WRDS tables, optionally with the given subsets of
#' GVKEYs or PERMNOs. You can not use both GVKEYs and PERMNOs to filter
#' on, so make sure to use only one of the parameters.
#' This function sends a SQL query to the WRDS cloud, receives the results
#' and puts the results in the local database.
#' TODO Allow incremental updates?
#' @param conn The WRDS connection object
#' @param tables A vector of table names to update
#' @param gvkeys A vector of GVKEYs to filter on
#' @param permnos A vector of PERMNOs to filter on
wrdsUpdateTable <- function(conn, tables, gvkeys=c(), permnos=c()) {
    ## Checking options
    if (length(gvkeys) > 0 && length(permnos) > 0) {
        stop("Can not use both gvkeys and permnos to filter")
    }

    localdb <- .wrdsGetDb()

    batchSize <- 1e5
    for(table in tables) {
        if (DBI::dbExistsTable(localdb, table)) DBI::dbRemoveTable(localdb, table)

        j = 1;
        sql <- paste0("SELECT * FROM CRSP.", table)
        if (length(gvkeys)) {
            sql <- paste0(sql, " WHERE GVKEY IN ('", paste0(gvkeys, collapse="', '"), "')")
        }
        message(paste("Querying WRDS for", table, "data"))
        res <- DBI::dbSendQuery(conn, sql)
        while(TRUE) {
            message(paste("...", as.integer(j * batchSize)))
            data <- tryCatch({
                data <- fetch(res, n=batchSize)
            }, error = function(e) {
                warning(paste("An exception was thrown:", e))
                return(data.frame())
            })

            if (nrow(data) == 0) break;

            DBI::dbWriteTable(localdb, table, data, append=TRUE)
            j = j + 1
        }

        DBI::dbClearResult(res)
    }
}

#' Collect GVKEYs of S&P500 constituents
#' As parameters you can give it the period of which you want to get the
#' S&P500 constituents. When a parameter is given, both are needed.
#' @param start Start date of the period (inclusive)
#' @param end End date of the period (inclusive)
wrdsGetSP500Gvkeys <- function(start=NA, end=NA) {
    localdb <- .wrdsGetDb()

    filter <- NA
    if (!is.na(start) && !is.na(end)) {
        filter <- .dateFilter(localdb, start, end, "sp.start", "sp.ending")
    }

    sql <- "SELECT DISTINCT GVKEY FROM CCMXPF_LNKHIST l INNER JOIN MSP500LIST sp ON l.LPERMNO=sp.PERMNO"
    if (!is.na(filter)) sql <- paste(sql, "WHERE", filter)

    res <- DBI::dbSendQuery(localdb, sql)
    gvkeys <- fetch(res, n=-1)

    gvkeys[, 1]
}

