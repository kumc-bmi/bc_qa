# cribbed from http://stackoverflow.com/a/10969107

require(knitr)
require(markdown)

load("bc_fetch_results.RData")

for (SITE in fetch$dataset$site) {
  print(SITE)
  rpt <- function(ext) paste0('report-', SITE, ext)
  knit('bc_excl.Rmd', rpt('.md'))
  markdownToHTML(rpt('.md'), rpt('.html'))
}

bc.team <- read.table(header=TRUE, text='email
dconnolly@kumc.edu
jhe@kumc.edu
tmcmahon@kumc.edu
e-chrischilles@uiowa.edu
bradley-mcdowell@uiowa.edu
tshireman@kumc.edu
vleonardo@kumc.edu
')

report.mail <- function(body,
                        sites=NULL, # all
                        flags='',
                        pyenv='/home/dconnolly/pyenv/bcqa',
                        cc=bc.team$email,
                        datasets='dataset.csv',
                        datadir=fetch$dataDir,
                      # TODO: document this least authority idiom
                      .system=system) {
  cmd <- 'PYENV/bin/python report_mail.py --datadir DATADIR --datasets DATASETS --cc CC FLAGS BODY SITES'
  bindings <- list(PYENV=pyenv,
                   DATASETS=datasets,
                   DATADIR=datadir,
                   CC=paste(cc, collapse=','),
                   FLAGS=flags,
                   # TODO: proper quoting for body. use system2?
                   BODY=body,
                   SITES=if(is.null(sites)) { '' } else { paste(sites, collapse=' ') })
  for (k in names(bindings)) {
    cmd <- gsub(k, bindings[[k]], cmd)
  }
  
  message('cmd:', cmd)
  .system(cmd)
}

report.mail("hi there!",
            sites=c('MCRF', 'KUMC'),
            flags="--dry-run")
