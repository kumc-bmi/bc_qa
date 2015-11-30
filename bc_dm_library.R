# ============================================================================
# GPC BC Datamart - Function Library
#
# Input Dataframes
#   dataset       - desriptors for site's input dataset 
#   tumor.site    - pt tumors listing
#   survey.sample - complex frame created by bc_excl
# 
# 25-Nov Genesis (Parceled out f/ original bc_site_datamart)
# 25-Nov SAVEPOINT (RMD file committed to TortoiseHg)
# ============================================================================

#=============================================================================
BCRup.Initialize.Site.Files <- function () {
  tmp.site.files <- data.frame(site='KUMC', source.db='KUMC-16-kumcBC.db')
  tmp.site.files <- rbind(tmp.site.files, data.frame(site='MCRF', source.db='MCRF-34-BC_Final_MCRF.db'))
  tmp.site.files <- rbind(tmp.site.files, data.frame(site='MCW', source.db='MCW-31-gpc_breastcancer_export_3_3_2015.db'))
  tmp.site.files <- rbind(tmp.site.files, data.frame(site='UIOWA', source.db='UIOWA-38-BRCA'))
  tmp.site.files <- rbind(tmp.site.files, data.frame(site='UMN', source.db='UMN-30-UMN BC SQLLite v2 with seq num.db'))
  tmp.site.files <- rbind(tmp.site.files, data.frame(site='UNMC', source.db='UNMC-37-UNMC_BCS_with560.db'))
  #tmp.site.files <- rbind(tmp.site.files, data.frame(site='UTHSCSA', source.db='UTHSCSA-35-bc_560.db'))
  tmp.site.files <- rbind(tmp.site.files, data.frame(site='UTSW', source.db='UTSW-32-bc_utsw20150304.db'))
  tmp.site.files <- rbind(tmp.site.files, data.frame(site='WISC', source.db='WISC-21-gpcnetworkwisc_02_27_2.db'))
  message("Number of site files identified: ",nrow(tmp.site.files))
  return(tmp.site.files)
}

#=============================================================================
BCRup.Initialize.Col.Terms <- function () {
  tmp.col.terms  <- data.frame(term.1='Breast', term.2='Breast', col.name='Seer.Site.Breast', col.data.type='CHAR')
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0160', term.2='race 1', col.name='NAACCR.0160.Race.1', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0161', term.2='race 2', col.name='NAACCR.0161.Race.2', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0162', term.2='race 3', col.name='NAACCR.0162.Race.3', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0163', term.2='race 4', col.name='NAACCR.0163.Race.4', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0164', term.2='race 5', col.name='NAACCR.0164.Race.5', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0190', term.2='spanish', col.name='NAACCR.0190.Spanish', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0220', term.2='sex', col.name='NAACCR.0220.Sex', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0240', term.2='birth', col.name='NAACCR.0240.Birth.Date', col.data.type='DATE'))  # Date of birth
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0380', term.2='central', col.name='NAACCR.0380.Seqno.Central', col.data.type='CHAR'))  # Seqno Central
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0390', term.2='diagnosis', col.name='NAACCR.0390.Dx.Date', col.data.type='DATE'))  # Date of diagnosis
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0400', term.2='primary', col.name='NAACCR.0400.Primary.Site', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0410', term.2='laterality', col.name='NAACCR.0410.Laterality', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0440', term.2='grade', col.name='NAACCR.0440.Grade', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0490', term.2='confirmation', col.name='NAACCR.0490.Confirmation', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0521', term.2='morph', col.name='NAACCR.0521.Morphology', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0560', term.2='hospital', col.name='NAACCR.0560.Seqno.Hosp', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0610', term.2='class', col.name='NAACCR.0610.Class.Case', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0670', term.2='surg', col.name='NAACCR.0670.Surg.Prim.Site', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0820', term.2='positive', col.name='NAACCR.0820.Reg.Nodes.Pos', col.data.type='CHAR'))  # Regional Nodes Positive
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='0830', term.2='examine', col.name='NAACCR.0830.Reg.Nodes.Examined', col.data.type='CHAR'))  # Regional Nodes Examined
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='1750', term.2='contact', col.name='NAACCR.1750.Last.Contact.Date', col.data.type='DATE'))  
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='1760', term.2='vital', col.name='NAACCR.1760.Vital.Status', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='1860', term.2='recurrence', col.name='NAACCR.1860.Recurrence.Date', col.data.type='DATE'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='1861', term.2='flag', col.name='NAACCR.1861.Recurrence.Date.1st.Flag', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='2850', term.2='dx', col.name='NAACCR.2850.CSMets.Dx', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='2860', term.2='eval', col.name='NAACCR.2860.CSMets.Eval', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='2869', term.2='factor', col.name='NAACCR.2869.HER2.CSSSF15', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='2876', term.2='factor', col.name='NAACCR.2876.MS.Method.CSSSF22', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='2877', term.2='factor', col.name='NAACCR.2877.MS.Result.CSSSF23', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='2880', term.2='factor', col.name='NAACCR.2880.ER.CSSSF01', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='2890', term.2='factor', col.name='NAACCR.2890.PR.CSSSF02', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='2940', term.2='AJCC-6', col.name='NAACCR.2940.AJCC6.T', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='3000', term.2='AJCC-6', col.name='NAACCR.3000.AJCC6.Stage', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='3020', term.2='SS2000', col.name='NAACCR.3020.SS2000', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='3400', term.2='AJCC-7', col.name='NAACCR.3400.AJCC7.T', col.data.type='CHAR'))
  tmp.col.terms <- rbind(tmp.col.terms, data.frame(term.1='3430', term.2='AJCC-7', col.name='NAACCR.3430.AJCC7.Stage', col.data.type='CHAR'))
  message("Number of search terms identified: ",nrow(tmp.col.terms))
  return(tmp.col.terms)
}

#=============================================================================
BCRup.AddVariableToDatamart <- function(p.datamart, p.site.variables, p.site.ptobs, 
                                        p.code.string.1, p.code.string.2, 
                                        p.new.col.name, p.code.data.type) {

  # NOTE: Search terms must be found in both 'variable name' and 'concept path'
  # NOTE: Ignores case and punctuation in the concept path  
  
  log.record <- list("")
  tmp.valid.terms.flag <- TRUE
  tmp.new.col.descriptor.name <- paste0(p.new.col.name,".Descriptor")  # Used for CHAR data types
  tmp.datamart <- p.datamart
  
  # Check for matching variable -----------------------------------------------------
  message("Checking variable to be added: ",p.new.col.name) 
  # Ensure only 1 variable matches search terms
  message("... searching variable names containing: '",
          p.code.string.1,"' & '",p.code.string.2,"'")
  tmp.var.found.cnt <- nrow(subset(p.site.variables, 
                          grepl(p.code.string.1,concept_path,ignore.case=TRUE) &
                          grepl(p.code.string.2,concept_path,ignore.case=TRUE) ))  
  message("Number of variables found matching search terms: ",tmp.var.found.cnt)
                          
  # Variable not found!
  if (tmp.var.found.cnt == 0) {
    log.msg <- "Unable to find a variable matching search terms."
    log.action.taken <- "WARNING: Variable values will be set to NA."
    log.record <- data.frame(p.new.col.name,p.code.string.1,p.code.string.2,
                                log.msg,log.action.taken)
    v.script.log <<- rbind(v.script.log, 
                          log.record) 
    tmp.valid.terms.flag <- FALSE
    } 
  
  # More than ONE variable found!
  if (tmp.var.found.cnt > 1) {
    log.msg <- "Collision where multiple variables found matching search strings."
    log.action.taken <- "WARNING: Values from multiple variables merged into one column."
    log.record <- data.frame(p.new.col.name,p.code.string.1,p.code.string.2,
                           log.msg,log.action.taken)
    v.script.log <<- rbind(v.script.log, 
                          log.record) 
    }
  
  # Search and load any matching observation facts -----------------------------------------------------  
  message("Checking patient data (observation facts) for populated concept codes: ",p.new.col.name) 
  message("... searching facts for concept paths containing: '",
          p.code.string.1,"' & '",p.code.string.2,"'") 
  tmp.new.col.facts <- subset(p.site.ptobs, 
                              grepl(p.code.string.1,code.path,ignore.case=TRUE) &
                              grepl(p.code.string.2,code.path,ignore.case=TRUE) )   
  tmp.new.col.facts <- unique(tmp.new.col.facts[,c("patient.num","encounter.num",
                                                   "code","code.name","nval","start.date.char")]) 
  tmp.ncf.cnt <- nrow(tmp.new.col.facts)  # Necessary for knitr markdown 
  message("... number of unique records found: ",tmp.ncf.cnt)
  message("... number of unique values founds: ",length(unique(tmp.new.col.facts$code)))
  
    
  # Check for matching observation facts -----------------------------------------------------  
  if (tmp.valid.terms.flag) {
    if (! (p.code.data.type %in% c("CHAR","NUM","DATE"))) {
      log.msg <- "Datatype specified is not supported."
      log.action.taken <- "WARNING: Using simple code values for column."
      log.record <- data.frame(p.new.col.name,p.code.string.1,p.code.string.2,
                                  log.msg,log.action.taken)
      v.script.log <<- rbind(,v.script.log,log.record)
      }
    
    if (tmp.ncf.cnt > nrow(p.datamart)) {
        message("... verifying cardinality of result set for variable requested.")
        log.msg <- "Cardinality of variable does not correspond with desired result set."
        log.action.taken <- "WARNING: First value has arbitrarily been selected."
        log.record <- data.frame(p.new.col.name,p.code.string.1,p.code.string.2,
                                     log.msg,log.action.taken)
        v.script.log <<- rbind(v.script.log, 
                               log.record) 
            
        tmp.new.col.facts <- aggregate(tmp.new.col.facts,
                                    by=list(tmp.new.col.facts$encounter.num),
                                    FUN=head,1)
                
        tmp.valid.terms.flag <- TRUE
    } else {
      # Ensure observation facts exist for search terms
      message("ncf: ",tmp.ncf.cnt)
      if (tmp.ncf.cnt == 0) {
        log.msg <- "Unable to locate any facts for search strings in concept paths."
        log.action.taken <- "WARNING: Variable values will be set to NA."
        log.record <- data.frame(p.new.col.name,p.code.string.1,p.code.string.2,
                                     log.msg,log.action.taken)
        v.script.log <<- rbind(v.script.log, 
                               log.record) 
        tmp.valid.terms.flag <- FALSE
      }
    }
  }

  #------------------------------------------------------------------------------------
  # All conditions handled, proceed w/ adding variable values to datamart 
  # -- Invalid terms will have NAs loaded
  #-----------------------------------------------------------------------------------
  if (tmp.valid.terms.flag) {
    # Handle based on data type
    if (p.code.data.type == "NUM") {
      tmp.new.col.facts <- tmp.new.col.facts[,c("patient.num","encounter.num","nval")]
      tmp.new.col.facts <- setNames(tmp.new.col.facts,c("patient.num","encounter.num",
                                                      p.new.col.name))
    } else {
      if (p.code.data.type == "DATE") {
        # Use start.date.char to get hh:mm:ss
        tmp.new.col.facts <- tmp.new.col.facts[,c("patient.num","encounter.num","start.date.char")]
        tmp.new.col.facts <- setNames(tmp.new.col.facts,c("patient.num","encounter.num",
                                                      p.new.col.name))
      } else {
        if (!(p.code.data.type %in% c("NUM","DATE"))) {  # Handle as CHAR, includes unsupported data types
          message("... handling as type CHAR")
          tmp.new.col.facts <- tmp.new.col.facts[,c("patient.num","encounter.num","code","code.name")]
          tmp.new.col.facts <- setNames(tmp.new.col.facts,c("patient.num","encounter.num",
                                                      p.new.col.name,tmp.new.col.descriptor.name))
        } 
      }    
    }
    tmp.new.col.facts <- unique(tmp.new.col.facts)
    tmp.datamart <- merge(tmp.datamart, tmp.new.col.facts,
                          all.x=TRUE)  # don't prune on join mis-matches         
    log.msg <- "Search terms found."
    log.action.taken <- "Successfully added new column!"
    log.record <- data.frame(p.new.col.name,p.code.string.1,p.code.string.2,
                                 log.msg,log.action.taken)
    message(log.action.taken)
    v.script.log <<- rbind(v.script.log, 
                           log.record)  # Assign value to global variable
  } else { # Terms are not valid, so simply return NAs instead
    message("... assigning NAs to invalid term for variable: ",p.new.col.name)
    tmp.datamart[,c(p.new.col.name)] <- NA
    if (!(p.code.data.type %in% c("NUM","DATE"))) {  # Handle as CHAR, includes unsupported data types
      tmp.datamart[,c(tmp.new.col.descriptor.name)] <- NA
      }
  }
  return(tmp.datamart)
}

#=============================================================================
BCRup.DescribeDatamartColumn <- function(p.datamart,p.col.name) {
# Note - ignores case and punctuation n the concept path

     # By-pass descriptor columns, as they are merged w/ their root values
     if (grepl(".Descriptor$",p.col.name)) {
       return("Skipping over 'descriptor' column")
     }
     
     # Default data type
     p.col.datatype <- "CHAR"
               
     # Check for NAs
     tmp.col.found.flag <- (nrow(p.datamart[! is.na(p.datamart[,p.col.name] ), ]) > 0)
     if  (! tmp.col.found.flag) {
       message("No 'Descriptive Analysis', as only NAs are present for column: ",tmp.col.name) 
       return("No 'Descriptive Analysis' performed")
       }
     
     # Handle GPC eligibility variables by determining data type
     if (grepl("^gpc.",p.col.name)) {
       if (grepl(".date.",p.col.name)) {
          p.col.datatype <- "DATE"
          } else {
            if (grepl(".dx.age$",p.col.name)) {
              p.col.datatype <- "NUM"
            }
          }
     } else { # Handle as NAACCR variable
         p.col.datatype <- v.col.terms$col.data.type[strtrim(v.col.terms$col.name,11)==strtrim(p.col.name,11)]
     }
  
    # Convert dates from character to date format
    if (p.col.datatype == "DATE") { 
       p.datamart[,p.col.name] <- as.POSIXct(p.datamart[,p.col.name],"%y-%m-%d %T")
       } 
    
    if ((p.col.datatype == "DATE") | (p.col.datatype == "NUM")) {
       print(summary(p.datamart[,p.col.name]))
    } else {# Handle as character
       # Descriptive analysis for variable added
       tmp.next.col <- which(colnames(p.datamart)==p.col.name) + 1
       p.datamart$col.expanded <- p.datamart[,p.col.name]
       # Add descriptor, if not a GPC Eligibility variable
       if (! grepl("^gpc.",p.col.name)) {
         p.datamart$col.expanded <- paste0(p.datamart[,p.col.name]," :: ",p.datamart[,tmp.next.col])
         }
       tmp.frequency.table <- as.data.frame(addmargins(table(p.datamart$col.expanded)))
       tmp.frequency.table <- tmp.frequency.table[,c("Freq","Var1")]  # Switch column order
       colnames(tmp.frequency.table)[colnames(tmp.frequency.table)=="Var1"] <- tmp.col.name
       colnames(tmp.frequency.table)[colnames(tmp.frequency.table)=="Freq"] <- "Enctrs"
  
       print(tmp.frequency.table, right=FALSE)  
       }
   
}

#=============================================================================
BCRup.VisualizeDatamartColumn <- function(p.datamart,p.col.name) {
# Note - ignores case and punctuation n the concept path 

     # By-pass descriptor columns, as they are merged w/ their root values
     if (grepl(".Descriptor$",p.col.name)) {
       return("Skipping over 'descriptor' column")
     }
  
     tmp.col.found.flag <- (nrow(p.datamart[! (is.na(p.datamart[,p.col.name])),]) > 0)
     if  (! tmp.col.found.flag) {
       message("No 'Visual Analysis', as only NAs are present for column: ",p.col.name) 
       return("No 'Visual Analysis' performed")
       }

     # Remove any NAs from consideration (as they break the charts)
     p.datamart <- p.datamart[! (is.na(p.datamart[,p.col.name])),]
     p.col.datatype <- "CHAR"  # Default
     # Handle GPC eligibility variables by determining data type
     if (grepl("^gpc.",p.col.name)) {
       if (grepl(".date.",p.col.name)) {
          p.col.datatype <- "DATE"
          } else {
            if (grepl(".dx.age$",p.col.name)) {
              p.col.datatype <- "NUM"
            }
          }
     } else { # Handle as NAACCR variable
       p.col.datatype <- v.col.terms$col.data.type[strtrim(v.col.terms$col.name,11)==strtrim(p.col.name,11)]
     }          

    # Convert dates from character to date format
    if (p.col.datatype == "DATE") { 
      message("DATE encountered: ",p.col.name)
       p.datamart[,p.col.name] <- as.POSIXct(p.datamart[,p.col.name],"%y-%m-%d %T")
       } 
    
    if (p.col.datatype == "DATE") {
       hist(p.datamart[,p.col.name],
            breaks="weeks", format="%b %y",
            freq=TRUE,
            main=p.col.name,
            col=c("lightsteelblue"),
            xlab="")
    } else {
      if (p.col.datatype == "NUM") {
         hist(p.datamart[,p.col.name],
              breaks=max(p.datamart[,p.col.name]),
              freq=TRUE,
              main=p.col.name,
              col=c("lightsteelblue"),
              xlab="")
      } else {# Handle as character
        tmp.frequency.table <- as.data.frame(addmargins(table(p.datamart[,p.col.name])))
        tmp.frequency.table <- tmp.frequency.table[,c("Freq","Var1")]
        colnames(tmp.frequency.table)[colnames(tmp.frequency.table)=="Var1"] <- p.col.name
        colnames(tmp.frequency.table)[colnames(tmp.frequency.table)=="Freq"] <- "Enctrs"
           
        # Charts
        pie(table(p.datamart[,p.col.name]),
            main=p.col.name)  
       
        p.datamart$col.expanded <- p.datamart[,p.col.name]
        # Add descriptor, if not a GPC Eligibility variable
        if (! grepl("^gpc.",p.col.name)) {
          tmp.next.col <- which(colnames(p.datamart)==p.col.name) + 1
          p.datamart$col.expanded <- paste0(p.datamart[,p.col.name]," :: ",p.datamart[,tmp.next.col])
          }
       
        tmp.frequency.table2 <- aggregate(p.datamart$encounter.num, 
                                          by=list(p.datamart$col.expanded), 
                                         function(x) length(unique(x)))
       
        tmp.frequency.table2 <- tmp.frequency.table2[order(tmp.frequency.table2$x,decreasing=TRUE),]
        tmp.row.cnt <- nrow(tmp.frequency.table2)
        tmp.max.analysis.cnt <- min(7,tmp.row.cnt)
        tmp.chart.subtitle <- "All Occurrences"
        if (tmp.row.cnt > tmp.max.analysis.cnt) {
            tmp.chart.subtitle <- paste0("Most Frequent Occurrences (Top ",tmp.max.analysis.cnt," of ",tmp.row.cnt,")")
        }
        tmp.frequency.table2 <- tmp.frequency.table2[1:tmp.max.analysis.cnt,]  
        op <- par(mar=c(5,4,3,2),bg="white")   # Bottom, left, top, right
        bp <- barplot(tmp.frequency.table2$x,
                       horiz=TRUE,
                       xlim=c(0,max(tmp.frequency.table2$x)*1.25),
                       col=c("lightsteelblue"),
                       #width allows all x labels to be shown
                       xlab="Encounters (Tumors)")         
        text(bp, x=0, labels=tmp.frequency.table2$Group.1, pos=4, cex=.85)
        mtext(side=3, line=1, p.col.name, font=2)
        mtext(side=3, line=0, tmp.chart.subtitle) 
        par(op)  # reset
        }
  }
}

#=============================================================================
BCRup.GetConsentedPtData <- function(p.site) {
  # Iowa: 100101 - 100360
  # KUMC: 150101 - 150360
  # UWISC: 200101 - 200360
  # UTSW: 250101 - 250360
  # MCW: 300101 - 300360
  # UNMC: 350101 - 350360
  # UMN: 400101 - 400360
  # MCRF: 450101 - 450360
  
  # Load site's allpt-allenctr datamart
  v.filename <- paste0(v.output.dir,"BCDatamart-AllPts-AllEnctrs-",p.site,".csv")
  message("Reading 'all-pt all enctr' datamart: ",v.filename)
  tmp.site.allpt.datamart <- read.csv(v.filename)
  message("... ",nrow(tmp.site.allpt.datamart)," rows read.")
  
  # Load site mapping file of consented study_ids to order_ids to patient_nums
  # || PATIENT_NUM|| ORDER_ID || STUDY_ID  || DATE_SHIFT ||
  v.study.mappings.dir <- '/d1/home/vleonardo/GPC-Development/bc-data-files/Phase1-StudyIdMappings/'
  v.site.consented.mapping <- read.csv(paste0(v.study.mappings.dir,p.site,"-patient-mapping.csv"))
  v.site.consented.mapping <- setnames(v.site.consented.mapping,
                                       old=c("patient_num","order_id","study_id","date_shift"),
                                       new=c("patient.num","gpc.order.id","gpc.study.id","gpc.date.shift"))
  nrow(v.site.consented.mapping)
  
  # Filter site datamart for consented pts
  tmp.sdpc <- merge(tmp.site.allpt.datamart,
                    v.site.consented.mapping,
                    by.x="patient.num",
                    by.y="patient.num")
  length(unique(tmp.sdpc$patient.num))
  
  # Move Study id and Date Shift to front of the pack
  tmp.sdpc <- tmp.sdpc[,c(3,ncol(tmp.sdpc)-1,ncol(tmp.sdpc)-2,1,2,ncol(tmp.sdpc),4:(ncol(tmp.sdpc)-3))]
  
  # Apply date shift
  tmp.sdpc$gpc.date.shift <- abs(tmp.sdpc$gpc.date.shift)
  tmp.sdpc$gpc.date.birth <- as.Date(tmp.sdpc$gpc.date.birth) + tmp.sdpc$gpc.date.shift
  message("... applying date shift for gpc.date.birth")
  tmp.sdpc$gpc.date.dx    <- as.Date(tmp.sdpc$gpc.date.dx) + tmp.sdpc$gpc.date.shift
  message("... applying date shift for gpc.date.dx")
  tmp.sdpc <- setnames(tmp.sdpc,
                       old=c("gpc.date.birth","gpc.date.dx"),
                       new=c("gpc.date.birth.actl","gpc.date.dx.actl"))
  for (i in v.col.terms$col.name[v.col.terms$col.data.type=="DATE"]) {
    message("... applying date shift for ",i)
    tmp.sdpc[,i] <- as.Date(tmp.sdpc[,i]) + tmp.sdpc[,"gpc.date.shift"]
    tmp.sdpc <- setnames(tmp.sdpc,
                         old=i,
                         new=paste0(i,".Actl"))  
  }
  
  tmp.site.consented.datamart <- tmp.sdpc
  length(unique(tmp.site.consented.datamart$patient.num))
  return(tmp.site.consented.datamart)
}

#=============================================================================
BCRup.Summary.Statistics <- function (p.datamart) {
  message('ALL-SITE DATAMART (Consented Pts Only)')
  message('')
  message('')
  message('Total Encounters By Site:')
  print(table(v.consented.datamart$gpc.site.name))
  message('')
  message('')
  message('Total Patients By Site:')
  print(table(unique(v.consented.datamart[,c("gpc.site.name","patient.num")])$gpc.site.name))
  message('')
  message('')
  message('Patients loaded:         ',length(unique(p.datamart$patient.num)))
  message('Encounters exported:     ',nrow(p.datamart),' (num of rows written)')
  message('Pts/Encounters eligible: ',length(p.datamart$gpc.enctr.eligible[p.datamart$gpc.enctr.eligible]))
  message('Dx Age (Mean):           ',format(mean(unique(p.datamart[,c("patient.num","gpc.dx.age")])$gpc.dx.age),digits=4))
  message('GPC Birth Date (Min):    ',min(p.datamart$gpc.date.birth.actl))
  message('NAACCR Birth Date (Min): ',min(p.datamart$NAACCR.0240.Birth.Date[! is.na(p.datamart$NAACCR.0240.Birth.Date)]))
  message('GPC Birth Date (Max):    ',max(p.datamart$gpc.date.birth))
  message('NAACCR Birth Date (Max): ',max(p.datamart$NAACCR.0240.Birth.Date[(! is.na(p.datamart$NAACCR.0240.Birth.Date)) & 
                                                                              (p.datamart$gpc.site.name != "UIOWA")]))
  message('GPC Dx Date (Min):       ',min(p.datamart$gpc.date.dx))
  message('NAACCR Dx Date (Min):    ',min(p.datamart$NAACCR.0390.Dx.Date[! is.na(p.datamart$NAACCR.0390.Dx.Date)]))
  message('GPC Dx Date (Max):       ',max(p.datamart$gpc.date.dx))
  message('NAACCR Dx Date (Max):    ',max(p.datamart$NAACCR.0390.Dx.Date[! is.na(p.datamart$NAACCR.0390.Dx.Date)]))
  
  message(' ')
  print(setNames(aggregate(cbind(patient.num,encounter.num)~gpc.site.name,
                           data=p.datamart,
                           function(x) length(unique(x))),
                 c("GPC Site","Patients","Enctr(Tumors)")))
}