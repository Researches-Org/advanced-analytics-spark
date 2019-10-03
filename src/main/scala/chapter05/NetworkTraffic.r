Sys.setenv(SPARK_HOME = "/path/to/spark")
Sys.setenv(JAVA_HOME = "/path/to/java")
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "4g"))

clusters_data <- read.df("/path/to/kddcup.data", "csv",inferSchema = "true", header = "false")

colnames(clusters_data) <- c(
"duration", "protocol_type", "service", "flag",
"src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent",
"hot", "num_failed_logins", "logged_in", "num_compromised",
"root_shell", "su_attempted", "num_root", "num_file_creations",
"num_shells", "num_access_files", "num_outbound_cmds",
"is_host_login", "is_guest_login", "count", "srv_count",
"serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate",
"same_srv_rate", "diff_srv_rate", "srv_diff_host_rate",
"dst_host_count", "dst_host_srv_count",
"dst_host_same_srv_rate", "dst_host_diff_srv_rate",
"dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
"dst_host_serror_rate", "dst_host_srv_serror_rate",
"dst_host_rerror_rate", "dst_host_srv_rerror_rate",
"label")

numeric_only <- cache(drop(clusters_data, c("protocol_type", "service", "flag", "label")))

kmeans_model <- spark.kmeans(numeric_only, ~ .,k = 100, maxIter = 40, initMode = "k-means||")

clustering <- predict(kmeans_model, numeric_only)

clustering_sample <- collect(sample(clustering, FALSE, 0.01))

str(clustering_sample)

clusters <- clustering_sample["prediction"]

the thing which did fix it was:

# https://stackoverflow.com/questions/15292905/how-to-solve-the-error-missing-required-header-gl-gl-h-while-installing-the-p
# sudo apt install libglu1-mesa-dev freeglut3-dev mesa-common-dev

# Download and unzip the latest FreeType2 from https://sourceforge.net/projects/freetype/files/ to your Downloads folder. Then from this folder:

# sudo make
# sudo make install

# Re-open R and install.packages("rgl") should work.  https://stackoverflow.com/questions/51146968/r-on-ubuntu-16-04-rgl-wont-install-proper

install.packages("rgl")

library(rgl)

random_projection <- matrix(data = rnorm(3*ncol(data)), ncol = 3)

num_clusters <- max(clusters)
palette <- rainbow(num_clusters)
colors = sapply(clusters, function(c) palette[c])
plot3d(projected_data, col = colors, size = 10)