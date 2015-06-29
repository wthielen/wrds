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
        stop("Package RJDBC can not be loaded")
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

    .jaddClassPath(sasCore)
    .jaddClassPath(sasDriver)

    driver <- RJDBC::JDBC("com.sas.net.sharenet.ShareNetDriver", sasDriver, identifier.quote="`")

    username <- ifelse(is.null(username), getOption("wrds.username"), username)
    password <- ifelse(is.null(password), getOption("wrds.password"), password)

    client <- DBI::dbConnect(driver, "jdbc:sharenet://wrds-cloud.wharton.upenn.edu:4016/", username, password)
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
