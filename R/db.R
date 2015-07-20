## Gets the connection object to the local WRDS database using SQLite
.wrdsGetDb <- function() {
    if (!('package:RSQLite' %in% search() || require('RSQLite', quiet=TRUE))) {
        stop("Package RJDBC could not be loaded")
    }

    dbPath = paste(Sys.getenv("HOME"), "wrds.db", sep=.Platform$file.sep)
    dbcon = dbConnect(SQLite(), dbPath)

    dbcon
}

## Computes the SQL filters to include everything
## in the given period.
## @param conn The JDBC connection object (for the dbQuoteString)
## @param fstart Start date of the filtered period
## @param fend End date of the filtered period
## @param fldStart Field name for the start date (depends on table/SQL)
## @param fldEnd Field name for the end date (depends on table/SQL)
.dateFilter <- function(conn, fstart, fend, fldStart, fldEnd) {
    fstart = DBI::dbQuoteString(conn, as.character(fstart))
    fend = DBI::dbQuoteString(conn, as.character(fend))

    case1 = paste(fldStart, "<=", fstart, "AND", fldEnd, ">=", fstart)
    case2 = paste(fldStart, ">=", fstart, "AND", fldStart, "<=", fend)

    paste0("(", case1, ") OR (", case2, ")")
}

