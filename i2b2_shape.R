## i2b2_shape - arrange i2b2 data in more traditional R shapes.

# See also i2b2-tidy.Rmd for design rationale and usage examples.

library(DBI)

data.dictionary <- function(
  conn,
  path.extra="^\\\\i2b2",
  q.path.levels="
    select distinct concept_path
    from concept_dimension
    order by concept_path",
  q.code.levels="
    select distinct concept_cd from observation_fact
    union 
    select distinct concept_cd from concept_dimension
    order by 1") {
  # Provides access to the data dictionary aspects of the i2b2 star schema.
  #
  # Args:
  #  conn: a DBI connection to an i2b2 star schema.
  #        Tested with SQLite from the HERON DataBuilder.
  #        
  #  path.extra: A regexp to strip from paths to normalize; typically
  #              to remove the initial segment corresponding
  #              to `c_table_cd` from the i2b2 TABLE_ACCESS table.
  #              Matched case-insensitively.
  #  q.path.levels: a SQL query to find all concept paths, which are used
  #                 as the levels of path factors.
  #  q.code.levels: a SQL query to find all concept codes, which are used
  #                 as the levels of code factors.
  # Returns:
  #   A list of:
  #     @@FIXME
  #     $path.levels: a character vector of all concept paths
  #     $code.levels: a character vector of concept codes
  #
  # TODO: provide names / labels corresponding to codes, paths
  # TODO: consider expanding to a vector or list of connections
  #       so that factor levels can be aggregated across sites.
  # TODO: consider extending prune.table.code to a regexp to
  #       handle multiple `c_table_cd` values.
  prune.path <- function(p) gsub(path.extra, '', p, ignore.case=TRUE)

  hier <- dbGetQuery(
    conn.site,
    "select descendant.concept_path descendant_path
          , descendant.concept_cd
          , ancestor.concept_path ancestor_path
     from concept_dimension descendant
     left join concept_dimension ancestor
       on descendant.concept_path like (ancestor.concept_path || '_%')
     ")
  path.levels <- prune.path(dbGetQuery(conn, q.path.levels)$concept_path)
  code.levels = dbGetQuery(conn, q.code.levels)$concept_cd
  
  hier$concept_cd = factor(hier$concept_cd, levels=code.levels)

  norm.path <- function(p) factor(prune.path(p), levels=path.levels)
  hier$ancestor_path <-norm.path(prune.path(hier$ancestor_path))
  hier$descendant_path <- norm.path(hier$descendant_path)
  
  obs.under <- function(obs, ancestor) {
    code <- subset(hier, ancestor_path == ancestor)$concept_cd
    subset(obs, concept_cd %in% code)
  }

  obs.at <- function(obs, path) {
    code <- subset(hier, descendant_path == path)$concept_cd
    subset(obs, concept_cd %in% code)
  }
  
  path.name <- dbGetQuery(
    conn,
    "select distinct concept_path, name_char
     from concept_dimension
     order by concept_path")
  path.name$concept_path <- norm.path(path.name$concept_path)

  code.path <- dbGetQuery(
    conn,
    "
    select distinct concept_cd, concept_path
    from concept_dimension
    group by concept_cd
    ")
  code.path$concept_cd <- factor(code.path$concept_cd,
                                 levels=code.levels)
  code.path$concept_path <- norm.path(code.path$concept_path)
  code.name <- function(coded) {
    ea <- unique(coded$concept_cd)
    ea.path <- merge(data.frame(concept_cd=ea), code.path, all.x=TRUE)
    if (nrow(ea.path) > length(ea)) {
      warning("ambiguous code(s):", duplicated(ea.path$concept_cd))
    }
    obs <- merge(merge(coded, ea.path, all.x=TRUE),
                 path.name, all.x=TRUE)
    if (any(is.na(obs$name_char))) {
      obs[is.na(obs$name_char)] <- obs$concept_cd[is.na(obs$name_char)]
    }
    obs
  }
  list(obs.under=obs.under,
       obs.at=obs.at,
       path.name=path.name,
       code.path=code.path,
       code.name=code.name,
       norm.path=norm.path,
       hier=hier,
       path.levels=path.levels,
       code.levels=code.levels)
}

per.encounter.nominal <- function(conn, code.levels) {
  # Builds a dataframe of per-encounter nominal data
  #
  # Args:
  #   conn: a DBI connection to an i2b2 star schema.
  #   code.levels: character vector of concept codes
  #
  # Returns:
  #   a dataframe with:
  #     $encounter_num: integer vector of encounter identifiers
  #     $concept_cd: factor of code values
  
  # The documented way to pick out nominal data, would be `valtype_cd='@'`
  # but it's missing in the data I'm looking at. So we use an alternative
  # approach:

  # In the nonimal case, concept_cd looks like question:answer
  # in numeric etc., it just looks like question:
  # and the answer (value) is in some other column such as nval_num.
  sql = "select distinct f.encounter_num
              , f.concept_cd
     -- TODO: , f.instance_num
     -- TODO: , f.modifier_cd
        from observation_fact f  
  where f.concept_cd like '%:_%' -- question:answer 
  "
  obs <- dbGetQuery(conn, sql)
  obs$concept_cd <- factor(obs$concept_cd, levels=code.levels)
  enc.check.dups(obs)
  obs
}

enc.check.dups <- function(obs) {
  # TODO
  #if (length(dups) > 0) {
  #  warning(paste("ambiguous encounter_num:", dups))
  #}
}

per.encounter.date <- function(conn, code.levels) {
  when <- dbGetQuery(
    conn,
    "select distinct encounter_num, concept_cd, start_date, end_date
    from observation_fact")
  
  when$start_date <- as.POSIXct(when$start_date)
  when$end_date <- as.POSIXct(when$end_date)
  when$concept_cd <- factor(when$concept_cd, levels=code.levels)
  enc.check.dups(obs)
  when
}

nominal.long <- function(obs, dd, items) {
  # Extract and label a subset of observations relevant to some items.
  #
  # Args:
  #   obs: a data frame with $concept_cd
  #        but no $variable, $concept_path, nor $name_char columns.
  #   dd: as from data.dictionary()
  #   items: a data dictionary with $concept_path and rownames()
  #
  # Returns:
  #   The union (rbind) for item in items of
  #   the subset of obs with codes.of(item$concept_path),
  #   with $variable set to rownames(item)
  #   and $name_char merged in from dd$code.name.
  by.path <- function(ix) {
    item <- items[ix, ]
    obs <- dd$obs.under(obs, item$concept_path)
    obs <- dd$code.name(obs)
    if(nrow(obs) > 0) {
      obs$variable <- rownames(item)    
    }
    obs
  }
  
  segments <- lapply(1:nrow(items), by.path)
  do.call(rbind, segments)
}


date.long <- function(obs, dd, items) {
  by.path <- function(ix) {
    item <- items[ix, ]
    obs <- dd$obs.at(obs, item$concept_path)
    obs <- dd$code.name(obs)
    if(nrow(obs) > 0) {
      obs$variable <- rownames(item)
    }
    obs
  }
  
  segments <- lapply(1:nrow(items), by.path)
  do.call(rbind, segments)
}
