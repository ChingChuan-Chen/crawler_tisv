# to download the data from http://tisvcloud.freeway.gov.tw/history/
# implementation: Rscript highwayDataDownloader.r
# no input or output
#
# by Jamal C. C. Chen
# created time: 20180705
if (any(commandArgs() == "--help")) {
  cat("\nThis script is used to download the data from\n")
  cat("http://tisvcloud.freeway.gov.tw/history/.\n")
  cat("To implement this script with Rscript highwayDataDownloader.r.\n\n")
  cat("The accepted options are\n")
  cat("\t--download-dir=<dir>  <dir> is the directory to save downloaded files.\n")
  cat("\t--recent-days=K Default is 11. It will download the data created in recent K days.\n")
  cat("\t--number-threads=<int> Default is future::availableCores().\n\tThe nubmer of threads to be used in downloading.\n")
  cat("\t--not-to-download=<string> Default is NULL.\n\tTo specify the folders not to download.\n\tThe separator is comma(,).\n")
  cat("ex:\n\tRscript highwayDataDownloader.r --download-dir=D:/freeway_data ")
  cat("--check-days=5 --number-threads=5 --not-to-download=icons,cms,cctv")
  q("no")
}

# check that installr is installed
if (!"installr" %in% rownames(installed.packages()))
  install.packages("installr")

# check that required packages are installed
installr::require2(xml2)
installr::require2(plyr)
installr::require2(pipeR)
installr::require2(lubridate)
installr::require2(stringi)
installr::require2(stringr)
installr::require2(future)
installr::require2(future.apply)

parseArgFunc <- function(args, checkItem) {
  params <- args %>>% `[`(str_detect(., str_c("^", checkItem, "=")))
  if (length(params) == 0L)
    return(NA_character_)
  if (length(params) > 1L)
    stop(sprintf("The multiple '%s' arguments are detected!", checkItem))
  return(params %>>% str_split("=") %>>% (.[[1]][2]))
}

# use "download_data" as savingDiv if R is under interactive mode.
# else check the arguments of command.
if (interactive()) {
  downloadDir <- "download_data"
  recentDays <- 11L
  numThreads <- 12L
  notDownloadFolders <- c("roadlevel", "icons", "cms", "cctv")
} else {
  cmdArgs <- commandArgs()
  downloadDir <- parseArgFunc(cmdArgs, "--download-dir")
  if (is.na(downloadDir))
    downloadDir <- "."
  recentDays <- parseArgFunc(cmdArgs, "--recent-days") %>>% as.integer
  if (is.na(recentDays))
    recentDays <- 11L
  numThreads <- parseArgFunc(cmdArgs, "--number-threads") %>>% as.integer
  if (is.na(numThreads))
    numThreads <- future::availableCores()
  notDownloadArg <- parseArgFunc(cmdArgs, "--not-to-download")
  if (is.na(notDownloadArg)) {
    notDownloadFolders <- NULL
  } else {
    notDownloadFolders <- str_split(notDownloadArg, ",")[[1]]
  }
}

needDownloadDir <- "TDCS_PINGLIN"
if (needDownloadDir %in% notDownloadFolders)
  needDownloadDir <- NULL

# create downloadDir if downloadDir does not exist
if (!dir.exists(downloadDir))
  dir.create(downloadDir)

# get path of script and
downloadDir <- normalizePath(downloadDir, "/")

# the root url of website
rootWebUrl <- "http://tisvcloud.freeway.gov.tw/history"

# set the number of threads
plan(multiprocess, workers = numThreads)

# get the index of url
# @param url the target url.
getIndexListFunc <- function(url, recentDays, needDownloadDir) {
  # get url content
  htmlDoc <- read_html(url, "UTF-8")
  # get all dirs and files
  indexList <- htmlDoc %>>%
    xml_find_all("//td[@class='indexcolname']") %>>%
    xml_children %>>%
    xml_attr("href")
  if (length(indexList) > 1L) {
    indexList <- indexList[-1L]
  } else {
    indexList <- NULL
    return(list(dir = NULL, file = NULL))
  }
  # get the created times
  createdTimes <- htmlDoc %>>%
    xml_find_all("//td[@class='indexcollastmod']") %>>%
    xml_text %>>% parse_date_time("ymd HM", quiet = TRUE) %>>% na.omit
  if (length(needDownloadDir) > 0L) {
    idx <- str_detect(indexList, str_c("^", needDownloadDir, collapse = "|"))
    createdTimes[idx] <- Sys.time()
  }
  # remove the created time is greater than today minus recents days
  indexList <- indexList[createdTimes > floor_date(Sys.time() - recentDays*86400, "day")]
  # remove Parent Directory
  if (length(indexList) == 0L) {
    # if there is no other dir or file then exit
    return(list(dir = NULL, file = NULL))
  }
  # convert filename to big5 if system OS is Windows
  if (installr::is.windows())
    indexList <- stri_conv(indexList, "UTF-8", "Big5")
  # return dirs / files
  isDir <- str_detect(indexList, "/$")
  return(list(dir = indexList[isDir], file = indexList[!isDir]))
}

# A function to download the files from the path in the url.
# @param url The root url.
# @param path The path to the files
# @param files The file list to download
downloadFileListFunc <- function(url, downloadFiles, downloadDir, retryCount = 15L, checkFileExist = FALSE) {
  # use future to download files
  future_lapply(downloadFiles, function(file){
    downloadUrl <- str_c(url, "/", file)
    savingFile <- str_c(downloadDir, "/", file)
    if (!dir.exists(str_c(downloadDir, "/", dirname(file))))
      dir.create(str_c(downloadDir, "/", dirname(file)), recursive = TRUE)
    if (!checkFileExist || !file.exists(savingFile)) {
      tryCnt <- 0L
      repeat {
        status <- tryCatch(download.file(downloadUrl, savingFile, quiet = TRUE),
                           error = function(e) e)
        if (status == 0L)
          break
        if ("error" %in% class(status)) {
          message("[", Sys.time(), "] Error occured when download files, the error message is ", status$message, ".")
          if (tryCnt >= retryCount) {
            break
          } else {
            message("[", Sys.time(), "] Now retry with retry count ", tryCnt, ".")
          }
        }
        tryCnt <- tryCnt + 1L
      }
    }
    invisible(NULL)
  })
  invisible(NULL)
}

# step 1: Download the files from root url.
rootIndex <- getIndexListFunc(rootWebUrl, recentDays, needDownloadDir)
rootIndex$dir <- setdiff(rootIndex$dir, str_c(notDownloadFolders, "/"))
message("[", Sys.time(), "] Download the files on ", rootWebUrl, "...")
downloadFileListFunc(rootWebUrl, rootIndex$file, downloadDir)

# step 2: list subfolder
message("[", Sys.time(), "] The folders to download are ", str_c(rootIndex$dir, collapse = ", "))
downloadDirs <- rootIndex$dir
downloadFiles <- NULL
while (length(downloadDirs) > 0L) {
  # remove first element
  path <- downloadDirs[1L]
  if (length(downloadDirs) >= 2L) {
    downloadDirs <- downloadDirs[2L:length(downloadDirs)]
  } else {
    downloadDirs <- NULL
  }
  # logging
  message("[", Sys.time(), "] Check the index list on ", rootWebUrl, "/", path, ".")
  # get subfolders
  pathIndex <- getIndexListFunc(str_c(rootWebUrl, "/", path), recentDays, needDownloadDir)
  # append download files
  if (length(pathIndex$file) > 0)
    downloadFiles <- c(downloadFiles, str_c(path, pathIndex$file))
  # append download dirs
  if (length(pathIndex$dir) > 0)
    downloadDirs <- c(downloadDirs, str_c(path, pathIndex$dir))
}

# step 3: download all files
message("[", Sys.time(), "] Download the files ...")
downloadFileListFunc(rootWebUrl, downloadFiles, downloadDir, checkFileExist = TRUE)
message("[", Sys.time(), "] Finish!")
