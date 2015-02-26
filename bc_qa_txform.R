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


patch.umn <- function(col,
                      fixes=bcterm$fixes) {
  expr <- col
  for (ix in 1:nrow(fixes)) {
    expr = paste0("replace(", expr, ", '", fixes$from[ix], "', '", fixes$to[ix], "')")
  }
  expr
}

sql.fact <- function(var.col) {
  # use distinct in case of modifiers
  paste0("
  select distinct f.encounter_num, f.patient_num, ", var.col, "
  from observation_fact f
  join concept_dimension cd
  on cd.concept_cd = f.concept_cd
  where ", patch.umn("cd.concept_path"),
         # patch for Abridged stuff
  " like (replace(:path, '\\i2b2\\Abridged\\Demographics', '%') || \'%\')
  ")
}

v.enc.nominal <- function(conn, var.path, var.name,
                          # default factor.pattern picks out last path segment
                          factor.pattern='\\\\([^\\]+)\\\\$') {
  sql <- sql.fact("cd.concept_path")
  per.enc <- dbGetPreparedQuery(conn, sql, bind.data=data.frame(path=var.path))
  
  p <- per.enc$concept_path
  # pick out part matched by factor.pattern
  per.enc$concept_path <- as.factor(substr(p, regexpr(factor.pattern, p), nchar(p)))
  
  names(per.enc)[3] <- var.name
  
  # Eliminate dups in case of polyhierarchy
  unique(per.enc)
}


mk.agg.by.pat <- function(code.pattern, sep='|') {
  function(conn, var.path, var.name) {
    pat.obs <- dbGetQuery(conn, "
                        select distinct patient_num, substr(cd.concept_path, length(:v)) tail
                        from observation_fact f
                        join concept_dimension cd
                        on cd.concept_cd = f.concept_cd
                        where cd.concept_path like (:v || '%')
                        ", bind.data=data.frame(v=var.path))
    pat.obs$code <- gsub(code.pattern, '\\1', pat.obs$tail)
    # concatenate (c) all the observations for a patient
    pat.obs.agg <- aggregate(code ~ patient_num, pat.obs, function(...) paste(c(...), collapse=sep))
    pat.obs.agg$code <- as.factor(pat.obs.agg$code)
    names(pat.obs.agg)[2] <- var.name
    pat.obs.agg$encounter_num <- NA  # expected by with.var.pat
    # print(head(pat.obs.agg))
    pat.obs.agg
  }
}


with.var <- function(data, conn, path, name,
                     get.var=v.enc.nominal) {
  out <- merge(data, get.var(conn, path, name),
        all.x=TRUE)  # don't prune on join mis-matches
  stopifnot(nrow(out) == nrow(data))
  out
}

with.var.pat <- function(data, conn, path, name,
                         get.var=v.enc.nominal) {
  v <- get.var(conn, path, name)
  drop.enc <- subset(v, select=-c(encounter_num))  # http://stackoverflow.com/a/5234201
  out <- merge(data, unique(drop.enc),
        by='patient_num',
        all.x=TRUE)  # don't prune on join mis-matches
  # message(nrow(out), ' =? ', nrow(data))
  stopifnot(nrow(out) == nrow(data))
  out
}


v.enc <- function(conn, var.path, var.name) {
  per.enc <- dbGetPreparedQuery(conn, sql.fact("strftime('%Y-%m-%d', f.start_date) start_date"),
                                bind.data=data.frame(path=var.path))
  per.enc$start_date <- as.Date(per.enc$start_date)
  
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
      print(paste0('Too many ', v, ' missing: ', pct, '%. ', var.excl[ix, 'concept_path']))
    }
  }
}


age.in.years <- function(date.birth, as.of=Sys.Date()) {
  as.numeric(difftime(as.of,
                      date.birth,
                      units="days")) / 365.25
  
}


bc.exclusions <- function(conn.site,
                          var.incl=bcterm$t.incl,
                          var.excl=bcterm$excl.all) {
  # All dates of diagnosis
  tumor.site <- v.enc(conn.site,
                      subset(bcterm$term204, grepl('0390 Date of Diagnosis', concept_path)
                      )$concept_path,
                      'date.dx')
  # Per-encounter date var.
  tumor.site <- with.var(tumor.site, conn.site,
                         bcterm$excl['date.birth', 'concept_path'], 'date.birth',
                         get.var=v.enc)
  # Per-patient variables
  for (v in c('vital.ehr', 'language')) {
    tumor.site <- with.var.pat(tumor.site, conn.site, var.excl[v,]$concept_path, v)
  }

  # Inclusion criteria
  for (v in rownames(var.incl)) {
      tumor.site <- with.var(tumor.site, conn.site, var.incl[v,]$concept_path, v)
  }

  # Per-encounter nominal variables
  for (v in rownames(var.excl)) {
    if (! v %in% c('stage', 'vital.ehr', 'language', 'date.birth'))
    tumor.site <- with.var(tumor.site, conn.site, var.excl[v,]$concept_path, v)
  }

  # Combinations
  tumor.site$vital <- vital.combine(tumor.site)
  tumor.site$stage <- stage.combine(tumor.site)
  
  message('TODO: check bc.exclusions(conn.site) against tumor.site')
  
  tumor.site
}

vital.combine <- function(tumor.site) {
  factor(
    ifelse(grepl('^.0', tumor.site$vital.tr) |
             # TODO: update per GPC demographics paths, when resolved
             grepl('D', tumor.site$vital.ehr), 'Y',
           ifelse(grepl('^.1', tumor.site$vital.tr), 'N', NA)
    ))
  
}

check.demographics <- function(tumor.site) {
  survey.sample <- tumor.site[, c('encounter_num', 'patient_num')]
  survey.sample$age <- NA
  survey.sample$adult <- FALSE
  
  if (any(!is.na(tumor.site$date.birth))) {
    survey.sample$age <- try(age.in.years(tumor.site$date.birth))
    survey.sample$adult <- survey.sample$age >= 18
  }
  survey.sample$female <- grepl('2', tumor.site$sex)
  survey.sample$not.dead <- ! tumor.site$vital == 'Y'
  survey.sample$english <- tumor.site$language == '\\ENGLISH\\'
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

check.cases <- function(tumor.site,
                        recent.threshold=subset(bcterm$dx.date, txform == 'deid' & label == 'expanded')$start) {
  survey.sample <- check.demographics(tumor.site)
  survey.sample$recent.dx <- tumor.site$date.dx >= recent.threshold
  survey.sample$bc.dx <-
    !is.na(tumor.site$seer.breast) | grepl('^.C50', tumor.site$primary.site)
  message('TODO: for primary site C50, exclude by histology')
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
  # skip encounter_num, patient_num, age
  col.crit <- 4:length(survey.sample)

  survey.sample.size <- as.data.frame(array(NA, c(4, length(col.crit) + 1)))
  names(survey.sample.size) <- c('total', names(survey.sample[, col.crit]))

  row.names(survey.sample.size) <- c('ind.pat', 'cum.pat',
                                     'ind.tumor', 'cum.tumor')
  
  patient_num <- survey.sample[, 'patient_num']
  
  survey.sample.size$total <- c(rep(length(unique(patient_num)), 2),
                                rep(length(patient_num), 2))
  
  cum.crit <- rep(TRUE, nrow(survey.sample))
  
  for (col in col.crit) {
    crit.name <- names(survey.sample)[col]
    crit <- survey.sample[, col]
    crit[is.na(crit)] <- TRUE
    survey.sample.size['ind.pat', crit.name] <- length(unique(patient_num[crit]))
    survey.sample.size['ind.tumor', crit.name] <- length(which(crit))
    cum.crit <- cum.crit & crit
    survey.sample.size['cum.pat', crit.name] <- length(unique(patient_num[cum.crit]))
    survey.sample.size['cum.tumor', crit.name] <- length(which(cum.crit))
  }

  survey.sample.size
}
