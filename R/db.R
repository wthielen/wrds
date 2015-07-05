.wrdsGetDb <- function() {
    if (!('package:RSQLite' %in% search() || require('RSQLite', quiet=TRUE))) {
        stop("Package RJDBC could not be loaded")
    }

    dbPath = paste(Sys.getenv("HOME"), "wrds.db", sep=.Platform$file.sep)
    dbcon = dbConnect(SQLite(), dbPath)

    dbcon
}
