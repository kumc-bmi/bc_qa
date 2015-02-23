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
