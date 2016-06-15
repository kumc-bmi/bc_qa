
test_functions <- TRUE

archive <- function(df, note="No notes", remove=FALSE) {
  # get the name of the data frame
  df_name <- deparse(substitute(df))
  if (!exists("arc")) {
    arc <<-list(documentation=NULL)
  }
  arc[[df_name]] <<- df
  arc$documentation <<- rbind(arc$documentation, data.frame(name=df_name, note=note))
  items_in_archive <- length(names(arc))
  size_of_archive <- format(object.size(arc), units="auto")
  m <- paste(df_name, " added. Archive has ",
             items_in_archive-1, " items and uses ",
             size_of_archive, ".\n", sep="")
  cat(m)
  print(arc$documentation)
  if (remove) rm(list=df_name, pos=1)
  return(invisible(names(arc))) # invisible prevents the names from printing again.
}

if (test_functions) {
  cat("\nSimple test.\n")
  abc <- data.frame(x=LETTERS[1:3], y=letters[1:3])
  archive(abc, "First three letters", remove=TRUE)
  def <- data.frame(x=LETTERS[4:6], y=letters[4:6])
  archive(def, "Next three letters", remove=TRUE)
  print(arc)
  rm(arc)
}

align_icd9_codes <- function(dx_old) {
  # This program strips out the dots and pads short
  # icd9 codes with zeros. It prints out anything
  # that is too short or too long.
  dx_new <- gsub(".","",dx_old,fixed=TRUE)
  dx_new <- ifelse(nchar(dx_new)==3,paste(dx_new,"0",sep=""),dx_new)
  dx_new <- ifelse(nchar(dx_new)==4,paste(dx_new,"0",sep=""),dx_new)
  cat("\n\nThe following diagnosis codes are invalid:\n")
  print(sort(unique(dx_new[nchar(dx_new)!=5])))
  cat(".\n")
  return(dx_new)
}

if (test_functions) {
  cat("\nSimple test.\n")
  tst <- align_icd9_codes(c("0014","V28.9","XX"))
  cat("\nOutput:", tst)
}

count_unique_patients <- function(df1, df2) {
  # Calculate the number of unique patient_num (PATIENT_NUM)
  # values in each gp (GP) in df1 and computes the proportion
  # relative to those found in df2.
  # This function will fail without proper warning if the data
  # frames do not have the expected variables in them.
  tmp1 <- df1
  names(tmp1) <- tolower(names(tmp1))
  tb1 <- table(tmp1$gp[!duplicated(tmp1$patient_num)])
  tmp2 <- df2
  names(tmp2) <- tolower(names(tmp2))
  tb2 <- table(tmp2$gp[!duplicated(tmp2$patient_num)])
  pct <- round(100*tb1/tb2)
  counts <- paste(names(tb1), ": ", tb1, "/", tb2, " (", pct, "%)\n", sep="")
  cat(counts, sep="")
}

library("knitr")
knit_hooks$set(timer = function(before, options, envir) {
  if (before) {
    current_time <<- Sys.time()
    m <- paste("Chunk ", options$label, " started at ", as.character(current_time), ".\n", sep="")
    cat(m)
    return(m)
  } else {
    elapsed_time <- Sys.time() - current_time
    elapsed_message <- paste(round(elapsed_time, 1), attr(elapsed_time, "units"))
    m <- paste("Chunk", options$label, "used", elapsed_message)
    message(m)
    if (exists("timing_log")) {
      timing_log <<- c(timing_log, m)
    } else {
      timing_log <<- m
    }
    write.table(timing_log, file="timing_log.txt", row.names=FALSE, col.names=FALSE)  
    return(m)
  }
}
) 

list_random_rows <- function(df,n=5) {
  # select and return first five rows, last five rows, and
  # a few random rows from a data frame.
  # If input is a vector, coerce it into a data frame
  df <- data.frame(df, stringsAsFactors=FALSE)
  new_list <- NULL
  nrows <- dim(df)[1]
  if (nrows<=3*n) {return(list(All_rows=df))}
  top_name <- paste("First",n,"rows",sep="_")
  mid_name <- paste("Random",n,"rows",sep="_")
  bot_name <- paste("Last",n,"rows",sep="_")
  selected_rows <- 1:n
  new_list[[top_name]] <- df[selected_rows, ]
  selected_rows <- sort(sample((n+1):(nrows-n),n))
  new_list[[mid_name]] <- df[selected_rows, ]
  selected_rows <- (nrows+1-n):nrows
  new_list[[bot_name]] <- df[selected_rows, ]
  return(new_list)
}

print_random_rows <- function(df, n=5) {
  # explicitly prints output from list_random_rows
  print(list_random_rows(df, n))
}

if (test_functions) {
  cat("\nSimple test.\n")
  print_random_rows(1:100)
  print_random_rows(data.frame(LETTERS, letters, stringsAsFactors=FALSE))
}

strip_specials <- function(x0) {
  # This function strips special characters from a character vector,
  # replacing most of them with an underscore.
  x0 <- gsub(" ","_",x0,fixed=TRUE)
  x0 <- gsub("-","_",x0,fixed=TRUE)
  x0 <- gsub('"',"_", x0,fixed=TRUE)
  x0 <- gsub('/',"_", x0,fixed=TRUE)
  x0 <- gsub('+',"_", x0,fixed=TRUE)
  x0 <- gsub('&',"_", x0,fixed=TRUE)
  return(x0)
}

if (test_functions) {
  cat("\nSimple test.\n")
  print(strip_specials(c("test one","test-two","test&three")))
}


