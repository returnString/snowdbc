
#' @export
connect_snowflake <- function(user, password, host, warehouse, role, database = NULL, schema = NULL, bootstrap_session = TRUE) {
	con <- DBI::dbConnect(
		odbc::odbc(),
		UID = user,
		PWD = password,
		Server = host,
		Warehouse = warehouse,
		Role = role,
		Driver = "Snowflake",
		Database = database,
		Schema = schema
	)

	if (bootstrap_session) {
		DBI::dbGetQuery(con, "alter session set quoted_identifiers_ignore_case = true")
	}

	con
}

#' @export
db_query_fields.Snowflake <- function(con, sql, ...) {
	fields <- dbplyr:::db_query_fields.DBIConnection(con, sql, ...)
	tolower(fields)
}

#' @export
sql_subquery.Snowflake <- function(con, from, name = "a", ...) {
	dbplyr:::sql_subquery.DBIConnection(con, from, name, ...)
}

#' @export
collect.tbl_Snowflake <- function(x, ...) {
	data <- dbplyr:::collect.tbl_sql(x, ...)
	colnames(data) <- tolower(colnames(data))
	data
}
