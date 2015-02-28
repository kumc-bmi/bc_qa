# TODO? use tumor rather than tumor.site for parameters

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

sql.fact <- function(colsexpr) {
  # use distinct in case of modifiers
  paste0("
  select distinct ", colsexpr, "
  from observation_fact f
  join concept_dimension cd
  on cd.concept_cd = f.concept_cd
  where ", patch.umn("cd.concept_path"),
         # patch for Abridged stuff
  " like (replace(:path, '\\i2b2\\Abridged\\Demographics', '%') || \'%\')
  order by f.patient_num, f.encounter_num
  ")
}

v.enc.nominal <- function(conn, var.path, var.name,
                          # default: extract last path segment
                          code.pattern='[^\\\\]+(?=\\\\+$)') {
  sql <- sql.fact("f.encounter_num, f.patient_num, cd.concept_path")
  per.enc <- dbGetPreparedQuery(conn, sql, bind.data=data.frame(path=var.path))

  p <- per.enc$concept_path
  pos.match <- regexpr(code.pattern, p, perl=TRUE)
  per.enc$concept_path <- substr(p, pos.match, pos.match + attr(pos.match, 'match.length') - 1)
  per.enc$concept_path[pos.match < 0] <- NA
  
  names(per.enc)[3] <- var.name
  
  # Eliminate dups in case of polyhierarchy
  unique(per.enc)
}

mk.agg.by.pat <- function(
  # default: extract last path segment
  code.pattern='[^\\\\]+(?=\\\\$)',
  sep='|') {
  function(conn, var.path, var.name) {
    pat.obs <- dbGetQuery(conn, sql.fact("f.patient_num, cd.concept_path"),
                          bind.data=data.frame(path=var.path))
    if (nrow(pat.obs) > 0) {
      # pick out part matched by code.pattern
      p <- pat.obs$concept_path
      pos.match <- regexpr(code.pattern, p, perl=TRUE)
      # message(paste(head(pos.match), collapse=", "),
      #        "length: ", paste(head(attr(pos.match, 'match.length')), collapse=", "))
      pat.obs$code <- substr(p, pos.match, pos.match + attr(pos.match, 'match.length') - 1)
      pat.obs$code[pos.match < 0] <- NA
      # concatenate (c) all the observations for a patient
      pat.obs.agg <- aggregate(code ~ patient_num, pat.obs, function(...) paste(c(...), collapse=sep))
      pat.obs.agg$code <- as.factor(pat.obs.agg$code)
      pat.obs.agg$encounter_num <- NA
    } else{
      pat.obs.agg <- data.frame(patient_num=NA, code=NA, encounter_num=NA)
      pat.obs.agg <- pat.obs.agg[-1,]
    }
    names(pat.obs.agg)[2] <- var.name
    
    # print(head(pat.obs.agg))
    pat.obs.agg
  }
}


with.var <- function(data, conn, path, name,
                     get.var=v.enc.nominal) {
  out <- merge(data, get.var(conn, path, name),
        all.x=TRUE)  # don't prune on join mis-matches
  stopifnot(nrow(out) == nrow(data))
  out[order(out$patient_num, out$encounter_num), ]
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
  per.enc <- dbGetPreparedQuery(
    conn, sql.fact("f.encounter_num, f.patient_num, strftime('%Y-%m-%d', f.start_date) start_date"),
    bind.data=data.frame(path=var.path))
  per.enc$start_date <- tryCatch(as.Date(per.enc$start_date), error=function(e) NA)
  
  names(per.enc)[3] <- var.name
  per.enc
}

v.enc.text <- function(conn, var.path, var.name) {
  per.enc <- dbGetPreparedQuery(conn, sql.fact("f.encounter_num, f.patient_num, f.tval_char"),
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
                          var.excl=bcterm$excl.all,
                          dx.path=subset(bcterm$term204, grepl('0390 Date of Diagnosis', concept_path))$concept_path) {
  # KLUDGE to get *something* from completely mis-aligned sites.
  tumor.site <- dbGetQuery(conn.site,
                           "select distinct encounter_num, patient_num
                         from observation_fact f
                           join concept_dimension cd
                           on f.concept_cd = cd.concept_cd
                           where concept_path like '%Cancer%'")
  
  # All dates of diagnosis
  tumor.site <- with.var(tumor.site, conn.site, dx.path, 'date.dx',
                         get.var=v.enc)
  # some sites don't have '0390 Date of Diagnosis'??
  # Fall back to '0400 Primary Site' or SEER Site Breast
  t <- merge(merge(tumor.site[, c('encounter_num', 'patient_num', 'date.dx')],
                   v.enc(conn.site, var.incl[1,]$concept_path, 'date.seer'),
                   all.x=TRUE),
             v.enc(conn.site, var.incl[2,]$concept_path, 'date.primary'),
             all.x=TRUE)
  
  tumor.site$date.dx[is.na(tumor.site$date.dx)] <- t$date.primary[is.na(tumor.site$date.dx)]
  tumor.site$date.dx[is.na(tumor.site$date.dx)] <- t$date.seer[is.na(tumor.site$date.dx)]
  
  # Per-encounter date var.
  tumor.site <- with.var(tumor.site, conn.site,
                         var.excl['date.birth', 'concept_path'], 'date.birth',
                         get.var=v.enc)
  
  # Per-patient variables
  # Handle multiple vital status per patient (e.g. dead per EHR, dead per SSA)
  for (v in c('vital.ehr', 'language')) {
    tumor.site <- with.var.pat(tumor.site, conn.site,
                               var.excl[v, 'concept_path'], v,
                               get.var=mk.agg.by.pat())
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
  tumor.site$stage.ajcc[grepl("999|900|888", tumor.site$stage.ajcc)] <- NA
  tumor.site$stage.ss[grepl("9", tumor.site$stage.ss)] <- NA
  
  # Combinations
  tumor.site$vital <- vital.combine(tumor.site)
  tumor.site$stage <- stage.combine(tumor.site)

  # message('TODO: check bc.exclusions(conn.site) against tumor.site')
  
  tumor.site
}


dx.span <- function(tumor) {
  stopifnot(nrow(tumor) > 0)
  stopifnot(any(!is.na(tumor$date.dx)))

  x <- aggregate(date.dx ~ patient_num, tumor, min)
  # could be fewer if some pat have no date.dx
  # message(nrow(x), ' <?= ', length(unique(tumor$patient_num)))
  stopifnot(nrow(x) <= length(unique(tumor$patient_num)))
  names(x)[2] <- 'first'  

  x <- merge(x, aggregate(date.dx ~ patient_num, tumor, max))
  names(x)[3] <- 'last'
  x$span <- x$last - x$first
  x <- merge(x, as.data.frame(table(tumor.site$patient_num, dnn='patient_num')))

  t <- merge(tumor[, c('encounter_num', 'patient_num')], x,
             all.x=TRUE)
  # message(nrow(tumor), ' x ', nrow(x), ' =?= ', nrow(t))
  stopifnot(nrow(t) == nrow(tumor))
  t
}

vital.combine <- function(tumor.site) {
    ifelse(grepl('^0', tumor.site$vital.tr) |
             # TODO: update per GPC demographics paths, when resolved
             grepl('D', tumor.site$vital.ehr), FALSE,
           ifelse(grepl('^1', tumor.site$vital.tr), TRUE, NA)
    )
}

check.demographics <- function(tumor.site,
                               adult.age.min=18) {
  survey.sample <- tumor.site[, c('encounter_num', 'patient_num')]

  vital <- aggregate(vital ~ patient_num, tumor.site, function(...) min(..., na.rm=TRUE))
  pat.dead <- vital$patient_num[vital$vital == 0]
  survey.sample$not.dead <- NA
  survey.sample$not.dead[survey.sample$patient_num %in% pat.dead] <- FALSE

  survey.sample$age <- NA
  survey.sample$adult <- FALSE
  if (any(!is.na(tumor.site$date.birth))) {
    survey.sample$age <- age.in.years(tumor.site$date.birth)
    survey.sample$adult <- survey.sample$age >= adult.age.min
  }

  # "Breast Cancer Cohort Characterization — Survey Sample" report
  # also requies EMR sex = female
  survey.sample$female <- grepl('2', tumor.site$sex)

  survey.sample
}


stage.combine <- function(tumor.site) {
  factor(
    ifelse(grepl('^7', tumor.site$stage.ajcc) |
             grepl('^7', tumor.site$stage.ss), 'IV',
           ifelse(grepl('^[1-6]', tumor.site$stage.ajcc) |
                    grepl('^[1-5]', tumor.site$stage.ss), 'I-III',
                  ifelse(grepl('^0', tumor.site$stage.ajcc) |
                           grepl('^0', tumor.site$stage.ss), '0',
                         ifelse(is.na(tumor.site$stage.ajcc) & is.na(tumor.site$stage.ss), NA, '?'))))
  )
}

check.cases <- function(tumor.site,
                        recent.threshold=subset(bcterm$dx.date, txform == 'deid' & label == 'expanded')$start) {
  stopifnot(nrow(tumor.site) > 0)
  survey.sample <- tumor.site[, c('encounter_num', 'patient_num')]
  survey.sample$bc.dx <-
    !is.na(tumor.site$seer.breast) | (
      grepl('^C50', tumor.site$primary.site) & solid.histology(tumor.site$morphology))
  survey.sample$recent.dx <- tumor.site$date.dx >= recent.threshold
  
  survey.sample$confirmed <- TRUE
  survey.sample$confirmed[! grepl('[124]', tumor.site$confirm)] <- FALSE
  survey.sample$other.morph <- excl.pat.morph(tumor.site)$ok
  survey.sample$stage.ok <- TRUE  # absent info, assume OK
  survey.sample$stage.ok[tumor.site$stage == 'IV'] <- FALSE

  survey.sample$span <- tryCatch(dx.span(tumor.site)$span, error=function(e) NA)
  month <- as.difftime(30, units="days")
  survey.sample$no.prior <- grepl('0[01]', tumor.site$seq.no) | survey.sample$span < 4 * month
  survey.sample <- merge(survey.sample, check.demographics(tumor.site),
                         all.x=TRUE)
  survey.sample[order(survey.sample$patient_num, survey.sample$encounter_num),
                c('patient_num', 'encounter_num',
                  'bc.dx', 'recent.dx',
                  # In order from "Breast Cancer Cohort Characterization -- Survey Sample" report
                  'female',
                  'span',
                  'no.prior',
                  'confirmed',
                  'other.morph',
                  'stage.ok',
                  'not.dead',
                  # That report doesn't seem to include age.
                  'age',
                  'adult')]
}

excl.pat.morph <- function(tumor.site,
                           morph='8520/2') {
  t <- subset(tumor.site, select=c(patient_num, morphology))
  if (any(!is.na(t$morphology))) {
    t$other <- ifelse(is.na(t$morphology), NA,
                      ifelse(grepl(morph, t$morphology, fixed=TRUE), 0, 1))
    message('@@excl.pat.morph', format(table(t$other)))
    ok <- aggregate(other ~ patient_num, data=t, max)
    # table(ok$other)    
  } else {
    ok <- data.frame(patient_num=NA, other=NA)
  }
  t$ok <- NA
  t$ok[t$patient_num %in% ok$patient_num[ok$other == 1]] <- TRUE
  t$ok[t$patient_num %in% ok$patient_num[ok$other == 0]] <- FALSE
  # addmargins(table(t$ok))
  t
}

solid.histology <- function(codes) {
  hist <- substr(as.character(codes), 1, 4)
  mesothelioma <- codes >= '9050' & codes <= '9055'
  Kaposi_sarcoma <- codes == '9140'
  lymphoma <- codes >= '9590' & codes < '9992'
  ! mesothelioma & ! Kaposi_sarcoma & ! lymphoma
}

count.cases <- function(survey.sample) {
  # encounter_num, patient_num, age are not (logical) criteria
  crit.names <- names(subset(survey.sample,
                             select=-c(encounter_num, patient_num, age, span)))
  
  survey.sample.size <- as.data.frame(array(NA, c(4, length(crit.names) + 1)))
  names(survey.sample.size) <- c('total', crit.names)

  row.names(survey.sample.size) <- c('ind.pat', 'cum.pat',
                                     'ind.tumor', 'cum.tumor')
  
  patient_num <- survey.sample[, 'patient_num']
  
  survey.sample.size$total <- c(rep(length(unique(patient_num)), 2),
                                rep(length(patient_num), 2))
  
  cum.crit <- rep(TRUE, nrow(survey.sample))
  
  for (crit.name in crit.names) {
    crit <- survey.sample[, crit.name]
    crit[is.na(crit)] <- TRUE
    survey.sample.size['ind.pat', crit.name] <- length(unique(patient_num[crit]))
    survey.sample.size['ind.tumor', crit.name] <- length(which(crit))
    cum.crit <- cum.crit & crit
    survey.sample.size['cum.pat', crit.name] <- length(unique(patient_num[cum.crit]))
    survey.sample.size['cum.tumor', crit.name] <- length(which(cum.crit))
  }

  survey.sample.size
}


reduce.logical <- function(data) {
  x <- rep(TRUE, nrow(data))
  for (col in names(data)) {
    y <- data[, col]
    y[is.na(y)] <- TRUE
    x <- x & y
  }
  x
}
