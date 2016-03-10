# How to Contribute

@@ https://github.com/blog/1184-contributing-guidelines

ref [good commit messages](https://informatics.kumc.edu/work/wiki/UsingVersionControl#GoodCommitMessages)

## Open Source License

see LICENSE@@

MIT or Apache2? at your option?

## R Coding Style

We're considering adopting [Google's R Style Guide][GR].

### Identifiers

**ISSUE**: Google says:
> Don't use underscores ( _ ) or hyphens ( - ) in identifiers.

But we deal a lot with SQL column names (esp. from i2b2) where underscores
are quite conventional. And REDCap prohibits dots. And dots have an
entirely different meaning in python, Java, PHP, etc. So let's lean
toward `snake_case`.

Likwise, use `data_file.csv` rather than `data-file.csv`; otherwise
filenames don't alphebetize, which is an annoyance.

 - use verbs for functions,
 - nouns for data

**open issue**: Regarding order of functions in a .R file,
  - alphabetical order may make them easier to find
  - "story telling" order may make them easier to understand



### Line Length

RStudio has a [Show margin option][80] under
*Tools -> Global Options -> Code -> Display*.

### Function Documentation

Google's conventions don't seem to lend themselves to auto-generated R help
the way Hadley's recommendations on [object documentation][odoc] do.

[GR]: https://google.github.io/styleguide/Rguide.xml
[80]: https://support.rstudio.com/hc/en-us/community/posts/207625357-Toggle-80-character-warning-line
[odoc]: http://r-pkgs.had.co.nz/man.html
