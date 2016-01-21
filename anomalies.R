library(rmongodb)
library(AnomalyDetection)


save_anomalies <- function(mongo_host, mongo_db, app_ids) {
    mongo <- mongo.create(host=mongo_host, db=mongo_db)
    docs = list()
    for (app_id in app_ids) {
        anomalies <- find_anomalies(mongo, app_id)
        docs = c(docs, Map(
            function(anns, name) {
                mongo.bson.from.list(list(name=name, anomalies=anns, app_id=app_id))
            },
            anomalies,
            names(anomalies)
        ))
    }
    mongo.remove(
        mongo,
        "appstats.anomalies",
        mongo.bson.from.list(list(app_id=list('$in'=app_ids)))
    )
    mongo.insert.batch(mongo, "appstats.anomalies", docs)
}


find_anomalies <- function(mongo, app_id) {
    top_names <- load_top_names(mongo, app_id, 50)
    anomalies <- as.list(top_names)
    names(anomalies) <- top_names
    anomalies <- lapply(anomalies, load_counts_data, mongo=mongo, app_id=app_id)

    # run algorithm, print all errors to stderr
    anomalies <- lapply(
        anomalies,
        function(...) {
            tryCatch(AnomalyDetectionTs(...), error=
                function(e) {
                    write(toString(e), stderr())
                    list(anoms=c())
                })
        },
        max_anoms=0.02, direction='pos', only_last='day', plot=FALSE
    )

    anomalies <- Filter(function(x) length(x$anoms) > 0, anomalies)
    anomalies <- lapply(anomalies, function(x) x$anoms$timestamp)
    return(anomalies)
}


load_top_names <- function(mongo, app_id, limit) {
    res <- mongo.find(
        mongo, "appstats.appstats_docs",
        query=list(app_id=app_id),
        sort=list(NUMBER_day=-1),
        fields=list(name=1),
        limit=limit
    )

    out = NULL
    while (mongo.cursor.next(res)){
        row <- mongo.bson.to.list(mongo.cursor.value(res))
        out <- c(out, row$name)
    }
    return(out)
}


load_counts_data <- function(name, mongo, app_id) {
    start_date <- Sys.time() - 3628800 # three 2-week periods: 3 * 14 * 24 * 60 * 60
    attr(start_date, "tzone") <- "UTC"
    query <- list(app_id=app_id, name=name, date=list("$gt"=start_date))

    res <- mongo.find(
        mongo, "appstats.appstats_apps_periodic-1",
        query=query, fields=list("date"=1, "real_time"=1, "NUMBER"=1)
    )

    out <- list(timestamp=NULL, count=NULL)
    while (mongo.cursor.next(res)){
        row <- mongo.bson.to.list(mongo.cursor.value(res))
        out$timestamp <- c(out$timestamp, row$date)
        out$count <- c(out$count, row$real_time / row$NUMBER)
    }

    return(data.frame(out))
}


args <- commandArgs(trailingOnly=TRUE)

mongo_host <- args[1]
mongo_db <- args[2]
app_ids <- as.list(args[-1:-2])

save_anomalies(mongo_host, mongo_db, app_ids)
