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


# nice HTML tables
library("xtable")
ht <- function(x,
               caption=NULL, NA.string='NA') {
  print(xtable(x, caption=caption),
        type='html',
        NA.string=NA.string,
        caption.placement='top',
        html.table.attributes='border=1')
}


patch.umn <- function(col) {
  fixes <- read.csv(textConnection(
    'from,to
\\I2B2\\Cancer Cases\\0,\\i2b2\\naaccr\\S:
\\I2B2\\Cancer Cases\\1,\\i2b2\\naaccr\\S:1
\\I2B2\\Cancer Cases\\SEER Site Summary,\\i2b2\\naaccr\\SEER Site
\\I2B2\\Demographics,\\i2b2\\Demographics
Type and Behav ICD-O-3,Type&Behav ICD-O-3
'))
  
  expr <- col
  for (ix in 1:nrow(fixes)) {
    expr = paste0("replace(", expr, ", '", fixes$from[ix], "', '", fixes$to[ix], "')")
  }
  expr
}

sql.fact <- function(var.col) {
  paste0("
  select f.encounter_num, f.patient_num, ", var.col, "
  from observation_fact f
  join concept_dimension cd
  on cd.concept_cd = f.concept_cd
  where ", patch.umn("cd.concept_path"),
         # patch for Abridged stuff
  " like (replace(:path, '\\i2b2\\Abridged\\Demographics', '%') || \'%\')
  ")
}

v.enc.nominal <- function(conn, var.path, var.name) {
  per.enc <- dbGetPreparedQuery(conn, sql.fact("substr(cd.concept_path, length(:path)) tail"),
                                bind.data=data.frame(path=var.path))
  per.enc$tail <- as.factor(per.enc$tail)
  
  names(per.enc)[3] <- var.name
  per.enc
}


with.var <- function(data, conn, path, name,
                     get.var=v.enc.nominal) {
  merge(data, get.var(conn, path, name),
        all.x=TRUE)  # don't prune on join mis-matches
}


with.var.pat <- function(data, conn, path, name,
                         get.var=v.enc.nominal) {
  merge(data, get.var(conn, path, name),
        all.x=TRUE)  # don't prune on join mis-matches
}


v.enc <- function(conn, var.path, var.name) {
  per.enc <- dbGetPreparedQuery(conn, sql.fact("f.start_date"),
                                bind.data=data.frame(path=var.path))
  # per.enc$start_date <- as.POSIXct(per.enc$start_date)
  
  names(per.enc)[3] <- var.name
  per.enc
}

v.enc.text <- function(conn, var.path, var.name) {
  per.enc <- dbGetPreparedQuery(conn, sql.fact("f.tval_char"),
                                bind.data=data.frame(path=var.path))
  
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


age.in.years <- function(date.birth, as.of=Sys.Date()) {
  as.numeric(difftime(as.of,
                      as.POSIXct(date.birth),
                      units="days")) / 365.25
  
}


bc.exclusions <- function(conn.site,
                          dx_path=bcterm$bc.dx.path,
                          var.excl=bcterm$excl.all) {
  # All NAACCR-related encounters
  tumor.site <- dbGetQuery(conn.site,
                           "select distinct encounter_num,
                         patient_num from observation_fact f
                         join concept_dimension cd
                         on cd.concept_cd = f.concept_cd
                         where cd.concept_path like '%naaccr%'")
  # Breast cancer diagnosis
  tumor.site <- with.var(tumor.site, conn.site,
                         bcterm$bc.dx.path, 'bc.dx',
                         get.var=v.enc)

  # Per-encounter nominal variables
  for (v in rownames(var.excl)) {
    if (! v %in% c('stage', 'deceased', 'deceased.ehr', 'deceased.ssa', 'date.birth'))
    tumor.site <- with.var(tumor.site, conn.site, var.excl[v,]$concept_path, v)
  }
  
  # Per-encounter date var.
  tumor.site <- with.var(tumor.site, conn.site,
                         bcterm$excl['date.birth', 'concept_path'], 'date.birth',
                         get.var=v.enc)

  # Per-patient variables
  for (v in c('deceased.ehr', 'deceased.ssa')) {
      tumor.site <- with.var.pat(tumor.site, conn.site, var.excl[v,]$concept_path, v)
  }

  # Combinations
  tumor.site$vital <- vital.combine(tumor.site)
  tumor.site$stage <- stage.combine(tumor.site)

  tumor.site
}

vital.combine <- function(tumor.site) {
  factor(
    ifelse(grepl('^.0', tumor.site$vital.tr) |
             !is.na(tumor.site$deceased.ehr) |
             !is.na(tumor.site$deceased.ssa), 'Y',
           ifelse(grepl('^.1', tumor.site$vital.tr), 'N', NA)
    ))
  
}

check.demographics <- function(tumor.site) {
  survey.sample <- tumor.site[, c('encounter_num', 'patient_num')]
  survey.sample$age <- NA
  survey.sample$adult <- FALSE
  
  if (any(!is.na(tumor.site$date.birth))) {
    survey.sample$age <- age.in.years(tumor.site$date.birth)
    survey.sample$adult <- ! survey.sample$age < 18
  }
  survey.sample$female <- grepl('2', tumor.site$sex)
  survey.sample$not.dead <- ! tumor.site$vital == 'Y'
  survey.sample
}


stage.combine <- function(tumor.site) {
  factor(
    ifelse(grepl('^.7', tumor.site$stage.ajcc) |
             grepl('^.7', tumor.site$stage.ss), 'IV',
           ifelse(grepl('^.[1-6]', tumor.site$stage.ajcc) |
                    grepl('^.[1-5]', tumor.site$stage.ss), 'I-III',
                  ifelse(grepl('^.0', tumor.site$stage.ajcc) |
                           grepl('^.0', tumor.site$stage.ss), '0',
                         ifelse(is.na(tumor.site$stage.ajcc) & is.na(tumor.site$stage.ss), NA, '?'))))
  )
}

check.cases <- function(tumor.site) {
  survey.sample <- check.demographics(tumor.site)
  survey.sample$bc.dx <- !is.na(tumor.site$bc.dx)
  survey.sample$confirmed <- grepl('\\1', tumor.site$confirm, fixed=TRUE)
  survey.sample$other.morph <- survey.sample$patient_num %in% check.morph(tumor.site)
  survey.sample$stage.ok <- ! tumor.site$stage == 'IV'
  survey.sample$no.prior <- grepl('0[01]', tumor.site$seq.no)
  survey.sample
}

check.morph <- function(tumor.site,
                        morph='8520/2') {
  morph.by.pat <- unique(tumor.site[, c('patient_num', 'morphology')])
  morph.by.pat$other.morph <-
    (duplicated(morph.by.pat$patient_num) |
       !grepl(morph, morph.by.pat$morphology, fixed=TRUE))
  
  morph.by.pat$patient_num[morph.by.pat$other.morph]
}

count.cases <- function(survey.sample) {
  col.crit <- 4:length(survey.sample)  # skip encounter_num, patient_num, age

  survey.sample.size <- as.data.frame(array(NA, c(2, length(col.crit))))
  names(survey.sample.size) <- names(survey.sample[, col.crit])
  row.names(survey.sample.size) <- c('independent', 'cumulative')
  
  cum.crit <- rep(TRUE, nrow(survey.sample))
  
  patient_num <- survey.sample[, 'patient_num']
  for (col in col.crit) {
    crit.name <- names(survey.sample)[col]
    crit <- survey.sample[, col]
    survey.sample.size['independent', crit.name] <- length(unique(patient_num[crit]))
    cum.crit <- cum.crit & crit
    survey.sample.size['cumulative', crit.name] <- length(unique(patient_num[cum.crit]))
  }

  survey.sample.size
}
