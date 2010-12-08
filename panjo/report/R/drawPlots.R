dta1.10 <- read.csv("../dta/dta1_5.csv")
dtaMat <- cbind(as.numeric(dta1.10[,1]), as.numeric(dta1.10[,2]),
                as.numeric(as.character(dta1.10[,3])), as.numeric(as.character(dta1.10[,4])))

dta.Big <- read.csv("../dta/dta_big.csv")
dtaMat <- as.data.frame(cbind(as.numeric(dta.Big[,1]), as.numeric(dta.Big[,2]),
                              as.numeric(as.character(dta.Big[,3])), as.numeric(as.character(dta.Big[,4]))))



dta1.10[,c(3,4)] <- dtaMat[,c(3,4)]
bySize <- split(dta1.10, dta1.10[,1])
byProc <- split(dta1.10, dta1.10[,2])

PROCS <- unique(dta1.10[,2])
NS <- unique(dta1.10[,1])
toPlotN <- c(1,2,5,6,8,10)

procColors <- 1:length(PROCS)
sizeColors <- 1:length(NS)


calcSpeedup <- function() {
    t0 <- byProc[[1]][,3]

    lapply(2:length(byProc), function(i) {
        t0/(byProc[[i]][,3])
    })
}

calcEfficiency <- function() {
    t0 <- byProc[[1]][,3]

    lapply(2:length(byProc), function(i) {
        t0/(byProc[[i]][,3]*byProc[[i]][,2])
    })
}

plotEfficiency <- function() {
    matplot(PROCS[-1], xx <- do.call("rbind", calcEfficiency()),
            type = "l", xlab = "Number of Processors",
            ylab = "Parallel Efficiency", col = sizeColors,
            lty = 1:length(sizeColors))
    legend(1.85, .435, legend = paste("N:", NS), lty = 1:length(sizeColors),
           fil = sizeColors)
}

plotSpeedup <- function() {
    matplot(PROCS[-1], do.call("rbind", calcSpeedup()), type = "l",
            xlab = "Number of Processors", ylab = "Speedup", col = sizeColors,
            lty = 1:length(sizeColors))
    legend(1, 19, legend = paste("N:", NS), lty = 1:length(sizeColors),
           fill = sizeColors)
}

##
## performance plot.
##
plotPerformance <- function() {
    matplot(NS, do.call("rbind", lapply(bySize, function(x) {
        x[,3]})),type = "l", col = 1:length(procColors),
            lty = 1:length(PROCS), xlab = "N", ylab = "Time (s)")
    legend(1000, 4000, legend = paste("P:", PROCS), lty = 1:length(PROCS), fill = procColors)
}

plotMPIPerformance <- function() {
    matplot(NS, do.call("rbind", lapply(bySize, function(x) {
        x[,4]/x[,3]}))[,-1],type = "l", col = procColors, lty = 2:length(PROCS),
            xlab = "N", ylab = "MPI Time / Total Time")
    legend(6000, .6, legend = paste("P:", PROCS[-1]), lty = 2:length(PROCS), fill = procColors)
}

pdf("../plots/speedup.pdf")
plotSpeedup()
dev.off()

pdf("../plots/efficiency.pdf")
plotEfficiency()
dev.off()

pdf("../plots/performance.pdf")
plotPerformance()
dev.off()

pdf("../plots/mpi_performance.pdf")
plotMPIPerformance()
dev.off()

bigByProc=split(dtaMat, dtaMat[,2])

pdf("../plots/big_performance.pdf")
plot(bigByProc[[1]][,1], bigByProc[[1]][,3], type = "l", xlim = c(4000, 41000), ylim = c(0, 15000),
     xlab = "N", ylab = "Total Time", col = 1)
points(bigByProc[[2]][,1], bigByProc[[2]][,3], type = "l", col = 2)
legend(5000,14000, legend = c("P = 50", "P = 100"), fill = c(1,2))
dev.off()

pdf("../plots/mpi_big_performance.pdf")
plot(bigByProc[[1]][,1], bigByProc[[1]][,4]/bigByProc[[1]][,3],
     type = "l", xlim = c(5000, 40000), xlab = "N", ylab = "MPI Time / Total Time", col = 1)
points(bigByProc[[2]][,1], bigByProc[[2]][,4]/bigByProc[[2]][,3], type = "l", col = 2)
legend(30000,.2, legend = c("P = 50", "P = 100"), fill = c(1,2))
dev.off()
