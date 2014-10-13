#
# olympics.R
# 
# Example R script to read in data
# about Olympics medals and generate
# a plot of the total number of medals won
# by country.
#
# Parameters:
#
#  1. input_path: location to load olympics medals data file from
#  2. output_path: location to write plot of total medals won. 
#        Should end in extension of desired plot type (e.g. png)
#

# set the width for output
options(width=400)

# load up the input arguments
args <- commandArgs(TRUE)
sprintf("Arguments received: %s", paste(args, collapse=" "))
if (length(args) != 2) {
    stop("Expected 2 arguments: input_path output_path")
}
input_path <- args[1]
output_path <- args[2]

# Install libraries
install.packages("plyr", repos="http://lib.stat.cmu.edu/R/CRAN/")
install.packages("ggplot2", repos="http://lib.stat.cmu.edu/R/CRAN/")
library(plyr)
library(ggplot2)

# Read input file
data <- read.csv(input_path)

# Rollup by Country
summarized <- ddply(data, c("Country"), summarise,
                    num_medals = sum(Total, rm.NA=TRUE))

# Graph the result
plt <- ggplot(summarized, 
              aes(x=reorder(Country, 
                            num_medals), 
                  y=num_medals)) + geom_bar(stat="identity") + coord_flip()

# Save it
ggsave(plt, file=output_path, width=8, height=12, units="in")
sprintf("Saved olympic medal graph to %s", output_path)
