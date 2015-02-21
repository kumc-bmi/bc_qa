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
  # per.enc$start_date <- as.POSIXct(per.enc$start_date)
  
  names(per.enc)[3] <- var.name
  per.enc
}

show.issues <- function(tumor.site, var.excl,
                        threshold=50) {
  for (ix in 1:nrow(var.excl)) {
    v <- row.names(var.excl)[ix]
    x <- tumor.site[[v]]
    qty <- length(x)
    absent <- qty - length(na.omit(x))
    pct <- round(100.0 * absent / qty, 2)
    if (pct > threshold) {
      print(paste0('Too many ', v, ' missing: ', pct, '%.'))
    }
  }
}

bc.exclusions <- function(conn.site,
                          dx_path='\\i2b2\\naaccr\\SEER Site\\Breast\\') {
  tumor.site <- v.enc(conn.site, dx_path, 'bc.dx')
  tumor.site <- with.var(tumor.site, conn.site, var.excl['sex',]$concept_path, 'sex')
  tumor.site <- with.var(tumor.site, conn.site, var.excl['seq.no',]$concept_path, 'seq.no')
  tumor.site <- with.var(tumor.site, conn.site, var.excl['confirm',]$concept_path, 'confirm')
  tumor.site <- with.var(tumor.site, conn.site, var.excl['morphology',]$concept_path, 'morphology')
  
  # TODO: factor out paths
  tumor.site <- with.var(
    tumor.site, conn.site,
    '\\i2b2\\naaccr\\S:11 Stage/Prognostic Factors\\3020 Derived SS2000\\', 'stage.ss')
  tumor.site <- with.var(
    tumor.site, conn.site,
    '\\i2b2\\naaccr\\S:11 Stage/Prognostic Factors\\3430 Derived AJCC-7 Stage Grp\\', 'stage.ajcc')  
  tumor.site$stage <- factor.combine(tumor.site$stage.ss, tumor.site$stage.ajcc)
  
  tumor.site <- with.var(
    tumor.site, conn.site,
    '\\i2b2\\naaccr\\S:4 Follow-up/Recurrence/Death\\1760 Vital Status\\',
    'vital.tr')
  
  tumor.site <- with.var.pat(
    tumor.site, conn.site,
    '\\i2b2\\Demographics\\Vital Status\\Deceased\\', 'deceased.ehr')
  tumor.site <- with.var.pat(
    tumor.site, conn.site,
    '\\i2b2\\Demographics\\Vital Status\\Deceased per SSA\\', 'deceased.ssa')
  tumor.site$vital <- factor.combine(tumor.site$vital.tr,
                                     tumor.site$deceased.ehr,
                                     tumor.site$deceased.ssa)
  tumor.site
}
