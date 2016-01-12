lib_dir = "/usr/local/lib/R/site-library"
repos = "https://cran.rstudio.com"

install.packages("devtools", lib=lib_dir, repos=repos)
devtools::install_github("twitter/AnomalyDetection", lib=lib_dir, repos=repos)
install.packages("rmongodb", lib=lib_dir, repos=repos)
