library(RSQLite)

bc_access <- function(getKey, pyenv, src,
                      # TODO: document this least authority idiom
                      .system=system) {
  fetchTemplate <- 'bc_access --fetch <api_key> logstdout'
  normalizeTemplate <- 'bc_access normalize *-*-* logstdout'
  exportTemplate <- 'bc_access --export <api_key>'
  
  api_key <- getKey$value()
  pyscript <- gsub('PYENV', pyenv,
                   gsub('SRC', src,
                        'PYENV/bin/python SRC/bc_access.py'))
  
  expand <- function(tpl) {
    gsub('bc_access', pyscript,
         gsub('<api_key>', api_key,
              gsub('logstdout', '2>&1', tpl)))
  }
  
  formdata <- .system(expand(exportTemplate), intern=TRUE)
  fetchlog <- .system(expand(fetchTemplate), intern=TRUE)
  normalizelog <- .system(expand(normalizeTemplate), intern=TRUE)
  
  # TODO: least-authority access to open data files
  list(formdata=formdata, fetchlog=fetchlog, normalizelog=normalizelog)
}


site.data <- function(target, dataDir, current) {  
  f <- file.path(dataDir, subset(current, site == target)$filename)
  dbConnect(SQLite(), dbname=f)
}


builder.summary <- function(conn,
                            v.name.max=50) {
  sql.summary <- '
  select v.concept_path, v.name_char,
  count(distinct patient_num) pat_qty, count(distinct encounter_num) enc_qty, count(*) fact_qty
  from observation_fact f
  join concept_dimension cd
  on cd.concept_cd = f.concept_cd
  join variable v
  on cd.concept_path like (v.concept_path || \'%\')
  group by v.concept_path, v.name_char
  '
  per.var <- dbGetQuery(conn, sql.summary)
  per.var$variable <- substr(strip.counts(per.var$name_char), 1, v.name.max)
  per.var
}

strip.counts <- function(text) {
  gsub('\\[.*', '', text)
}


v.enc.nominal <- function(conn, var.path, var.name) {
  sql.summary <- '
  select f.encounter_num, f.patient_num, substr(cd.concept_path, length(v.concept_path)) tail
  from observation_fact f
  join concept_dimension cd
  on cd.concept_cd = f.concept_cd
  join variable v
  on cd.concept_path like (v.concept_path || \'%\')
  where v.concept_path = ?
  '
  per.enc <- dbGetPreparedQuery(conn, sql.summary, bind.data=data.frame(path=var.path))
  per.enc$tail <- as.factor(per.enc$tail)
  
  names(per.enc)[3] <- var.name
  per.enc
}


with.var <- function(data, conn, path, name,
                     get.var=v.enc.nominal) {
  merge(data, get.var(conn, path, name),
        all.x=TRUE)  # don't prune on join mis-matches
}


factor.combine <- function(...) {
  factors <- list(...)
  x <- factors[[1]]
  for (ix in 2:length(factors)) {
    print(length(x))
    cx <- as.character(x)
    cy <- as.character(factors[[ix]])
    cxy <- paste(cx, cy)
    cxy <- ifelse(is.na(cx), cy, ifelse(is.na(cy), cx, cxy))
    x <- as.factor(cxy)
  }
  x
}


with.var.pat <- function(data, conn, path, name,
                         get.var=v.enc.nominal) {
  merge(data, get.var(conn, path, name),
        all.x=TRUE)  # don't prune on join mis-matches
}


v.enc <- function(conn, var.path, var.name) {
  sql.summary <- '
  select f.encounter_num, f.patient_num, f.start_date
  from observation_fact f
  join concept_dimension cd
  on cd.concept_cd = f.concept_cd
  join variable v
  on cd.concept_path like (v.concept_path || \'%\')
  where v.concept_path = ?
  '
  per.enc <- dbGetPreparedQuery(conn, sql.summary, bind.data=data.frame(path=var.path))
  per.enc$start_date <- as.POSIXct(per.enc$start_date)
  
  names(per.enc)[3] <- var.name
  per.enc
}

show.issues <- function(tumor.site,
                        threshold=50) {
  for (ix in 1:length(var.exclusion)) {
    v <- var.exclusion[ix]
    x <- tumor.site[[v]]
    qty <- length(x)
    absent <- qty - length(na.omit(x))
    pct <- round(100.0 * absent / qty, 2)
    if (pct > threshold) {
      print(paste0('Too many ', v, ' missing: ', pct, '%.'))
    }
  }
}
