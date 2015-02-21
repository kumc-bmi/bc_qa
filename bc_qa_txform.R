library(RSQLite)

site.data <- function(target,
                      .fetch=fetch) {  
  f <- file.path(.fetch$dataDir, subset(.fetch$file, site == target)$bc_db)
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
  select f.encounter_num, f.patient_num, substr(cd.concept_path, length(:path)) tail
  from observation_fact f
  join concept_dimension cd
  on cd.concept_cd = f.concept_cd
  where cd.concept_path like (:path || \'%\')
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
  where cd.concept_path like (? || \'%\')
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
                          dx_path=bcterm$bc.dx.path,
                          var.excl=bcterm$excl.all) {
  tumor.site <- v.enc(conn.site, dx_path, 'bc.dx')

  # Per-encounter variables
  for (v in rownames(var.excl)) {
    if (! v %in% c('stage', 'deceased', 'deceased.ehr', 'deceased.ssa'))
    tumor.site <- with.var(tumor.site, conn.site, var.excl[v,]$concept_path, v)
  }

  # Per-patient variables
  for (v in c('deceased.ehr', 'deceased.ssa')) {
      tumor.site <- with.var.pat(tumor.site, conn.site, var.excl[v,]$concept_path, v)
  }

  # Combinations
  tumor.site$stage <- factor.combine(tumor.site$stage.ss,
                                     tumor.site$stage.ajcc)  
  tumor.site$vital <- factor.combine(tumor.site$vital.tr,
                                     tumor.site$deceased.ehr,
                                     tumor.site$deceased.ssa)
  tumor.site
}
