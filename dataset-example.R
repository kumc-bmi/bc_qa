library(RSQLite)

f <- 'data-files/KUMC-16-kumcBC.db'

list(
  site='KUMC',
  bc_db=f,
  conn=dbConnect(SQLite(), dbname=f),
  content_length=NA,    # for audit use; arbitrary value will work for local use
  name='Dan Connolly',
  record_id=NA,         # for audit use by GPC BC team; arbitrary value will work
  issues_other=NA       # for audit use by GPC BC team; arbitrary value will work
)
