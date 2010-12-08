require(graph)
require(Rgraphviz)
require(ape)

tstNS <- matrix(1:36, nrow = 6)

tstAB <- matrix(c(0.000, 1.000, 2.000, 3.000, 3.000,
                  1.000, 0.000, 2.000, 3.000, 3.000,
                  2.000, 2.000, 0.000, 3.000, 3.000,
                  3.000, 3.000, 3.000, 0.000, 1.000,
                  3.000, 3.000, 3.000, 1.000, 0.000), nrow = 5)
colnames(tstAB) <- rownames(tstAB) <- c("A", "B", "C", "D", "E")

tst <- matrix(c(0,2,8,10,12,2,0,14,16,24,8,14,
                0,4,6,10,16,4,0,6,12,24,6,6,0),
              nrow = 5, ncol = 5)
colnames(tst) <- rownames(tst) <- as.character(1:ncol(tst))

tstA <- matrix(c(0,5,4,7,6,8,
              5,0,7,10,9,11,
              4,7,0,7,6,8,
              7,10,7,0,5,9,
              6,9,6,5,0,8,
              8,11,8,9,8,0), nrow = 6, ncol = 6)
colnames(tstA) <- rownames(tstA) <- as.character(1:ncol(tstA))


tstAA <- matrix(c(0,7,8,11,13,16,13,17,
                  7,0,5,8,10,13,10,14,
                  8,5,0,5,7,10,7,11,
                  11,8,5,0,8,11,8,12,
                  13,10,7,8,0,5,6,10,
                  16,13,10,11,5,0,9,13,
                  13,10,7,8,6,9,0,8,
                  17,14,11,12,10,13,8,0), 8,8)

colnames(tstAA) <- rownames(tstAA) <- as.character(1:ncol(tstAA))

grappaTst <- as.matrix(read.table("/Users/bullard/projects/panjo/dta/grappa.mat"))

require(graph)

njR <- function(mat) {
    valid <- rep(TRUE, ncol(mat))
    clusts <- ncol(mat)

    cnames <- colnames(mat)
    graph <- new("graphNEL", cnames)

    R <- rep(NA, ncol(mat))
    TreeScore <- 0
    
    while (clusts > 2) {

        for (i in 1:ncol(mat)) {
            R[i] = 0
            if (valid[i]) {
                for (j in 1:ncol(mat)) {
                    if (valid[j] && (i != j)) {
                        if (i < j)
                            R[i] <- R[i] + mat[i,j]
                        else
                            R[i] <- R[i] + mat[j,i]
                    }
                }
                R[i] <- R[i]/(clusts - 2)
            }
        }
        
        mn <- Inf
        mnij <- c(0,0)        
        for (i in 1:(ncol(mat) - 1)) {
            if (valid[i]) {
                for (j in (i + 1):ncol(mat)) {
                    if (valid[j]) {
                        Dij <- mat[i,j] - (R[i] + R[j])
                        
                        if (Dij < mn) {
                            mn <- Dij
                            mnij <- c(i,j)
                        }
                    }
                }
            }
        }

        i <- mnij[1]
        j <- mnij[2]
        dij <- mat[i,j]
        valid[j] <- FALSE

        for (l in 1:i) {
            if (valid[l]) {
                if (j < l)
                    mat[l,j] = .5*(mat[l,i] + mat[j,l] - dij)
                else
                    mat[i,l] = .5*(mat[l,i] + mat[l,j] - dij)
            }
        }

        for (l in (i+1):ncol(mat)) {
            if (valid[l]) {
                if (j < l)
                    mat[l,j] = .5*(mat[i,l] + mat[j,l] - dij)
                else
                    mat[i,l] = .5*(mat[i,l] + mat[l,j] - dij)
            }
        }
        
        dik <- .5*(dij + R[i] - R[j])
        djk <- dij - dik

        print(paste("grouping (",i-1,j-1, ") with distances(",
                    djk, ",", dik, ")", "R[i]=",R[i], "R[j]=",R[j], "clusters=", clusts));
        print(valid)
        print(mat)
        
        nn <- paste(cnames[i], cnames[j], sep = ":")
        graph <- addNode(nn, graph)
        graph <- addEdge(cnames[i], nn, graph, dik)
        graph <- addEdge(cnames[j], nn, graph, djk)

        cnames[i] <- nn        
        clusts <- (clusts - 1)

        TreeScore <- TreeScore + dij
    }

    leftover <- cnames[valid]
    pr <- paste(leftover, collapse = ":")
    graph <- addNode(pr, graph)
    graph <- addEdge(leftover[1], pr, graph)
    graph <- addEdge(leftover[2], pr, graph)

    ll <- which(valid)
    print(TreeScore + mat[ll[2], ll[1]])
    
    return(graph)
}

plotWithWeights <- function(graph) {
  nAttrs <- list() 
  eAttrs <- list()
  attrs <- getDefaultAttrs()
  
  ew <- as.character(round(unlist(edgeWeights(graph)), 3))
  ew <- ew[setdiff(seq(along = ew), removedEdges(graph))]
  names(ew) <- edgeNames(graph) 
  
  eAttrs$label <- ew
  tmp <- eAttrs$label
  tmp[1:length(tmp)] <- "8"
  eAttrs$fontsize <- tmp
  eAttrs$labelfontsize <- tmp

  plot(graph, nodeAttrs = nAttrs, edgeAttrs = eAttrs)
}


## string differences
calculateStringDifference <- function(s1, s2) {
    ss1 <- strsplit(s1, split = "")
    ss2 <- strsplit(s2, split = "")
    cnt <- mean(ss1[[1]] != ss2[[1]])
    
    return(-(.75)*log(1 - 4*cnt/3))
}

##
## calculate the jukes-cantor distance matrix
##
calculateJK <- function(seqs) {
  res <- matrix(NA, nrow = length(seqs)/2, ncol = length(seqs)/2)
  k <- l <- 1

  for (i in seq(2,length(seqs), by = 2)) {
    for (j in seq(2,length(seqs), by = 2)) {
      res[k,l] <- calculateStringDifference(seqs[[i]], seqs[[j]])
      l <- l + 1
    }
    l <- 1
    k <- k + 1
  }
  res
}
