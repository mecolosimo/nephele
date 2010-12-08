require(graph)
require(Rgraphviz)
require(ape)

spikes <- readLines("data/spikein_seqs.fasta")

## string differences
calculateStringDifference <- function(s1, s2) {
  ss1 <- strsplit(s1, split = "")
  ss2 <- strsplit(s2, split = "")
  cnt <- mean(ss1[[1]] != ss2[[1]])

  ## uses the JK - distance. 
  return(-(.75)*log(1 - 4*cnt/3))
  
}

parseNames <- function(nms) {
  as.character(lapply(nms, function(nm) {
    strsplit(nm, split = " ")[[1]][2]
  }))
}

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
  rownames(res) <- colnames(res) <-
    parseNames(seqs[seq(1,length(seqs), by = 2)])
  diag(res) <- Inf
  
  return(res)
}

which.mm <- function(m) {
  mn <- which.min(m)
  aa <- mn %% nrow(m)
  aa <- ifelse(aa == 0, nrow(m), aa)
  c(aa, floor(mn/nrow(m)) + 1)
}

neighborJoin <- function(dist) {
  nleaves <- nrow(dist)
  nnodes <- (2*nleaves - 1)

  graph <- new("graphNEL", as.character(1:nleaves))

  ## augment the distance matrix.
  adist <- matrix(NA, nrow = nnodes, ncol = nnodes)
  adist[1:nleaves, 1:nleaves] <- dist
  diag(adist) <- 0

  ## init the tree.
  tree <- rep(NA, nnodes)
  nodeCounts <- rep(NA, nnodes)
  nodeCounts[1:nleaves] <- 1

  ws <- 1:nleaves
  currentNode <- nleaves + 1
  j <- 1
  
  while(currentNode <= nnodes) {
    
    ## first transform the distance matrix.
    D <- adist[ws, ws]
    R <- rowSums(D)/(length(ws) - 2)
    D <- D - outer(R, R, "+")

    ## get min on subset of distance matrix.
    diag(D) <- Inf

    ## which pair is the minimum.
    mm <- which.mm(D)
    pr <- ws[c(mm[1], mm[2])]

    adist[currentNode, ws] <- adist[ws ,currentNode] <- sapply(ws, function(i) {
      .5*(adist[pr[1],i] + adist[pr[2],i] - adist[pr[1],pr[2]])
    })

    dik <- .5*(adist[pr[1], pr[2]] - R[mm[1]] + R[mm[2]])
    djk <- adist[pr[1], pr[2]] - dik
    
    graph <- addNode(as.character(currentNode), graph)
    graph <- addEdge(as.character(currentNode), as.character(pr[1]), graph, dik)
    graph <- addEdge(as.character(currentNode), as.character(pr[2]), graph, djk)

    ws <- c(currentNode, ws[-c(mm[1], mm[2])])
    currentNode <- currentNode + 1
  }
  graph <- addEdge(as.character(ws[1]), as.character(ws[2]), graph)
  
  return(graph)
}

upgma <- function(dist) {
  nleaves <- nrow(dist)
  nnodes <- (2*nleaves - 1)

  graph <- new("graphNEL", as.character(1:nleaves))
  
  ## augment the distance matrix.
  adist <- matrix(NA, nrow = nnodes, ncol = nnodes)
  adist[1:nleaves, 1:nleaves] <- dist
  diag(adist) <- Inf

  ## init the tree.
  tree <- rep(NA, nnodes)
  nodeCounts <- rep(NA, nnodes)
  nodeCounts[1:nleaves] <- 1
  
  ws <- 1:nleaves
  currentNode <- nleaves + 1
  j <- 1
  
  while(currentNode <= nnodes) {
    ## get min on subset of distance matrix.
    mm <- which.mm(adist)

    ## add to tree 
    tree[j:(j + 1)] <- mm
    nodeCounts[currentNode] <- (nodeCounts[mm[1]] + nodeCounts[mm[2]])
    
    ## to construct an R graph.
    graph <- addNode(as.character(currentNode), graph)
    graph <- addEdge(as.character(currentNode),
                     as.character(mm[1]), graph, adist[mm[1],mm[2]]/2)
    graph <- addEdge(as.character(currentNode),
                     as.character(mm[2]), graph, adist[mm[1],mm[2]]/2)
    
    ## compute new distances 
    for (i in ws[!(ws == mm[1] | ws == mm[2])]) {
      
      adist[i,currentNode] <- adist[currentNode, i] <-
        (nodeCounts[mm[1]]*adist[mm[1], i] + nodeCounts[mm[2]]*adist[mm[2], i])/
          (nodeCounts[mm[1]] + nodeCounts[mm[2]])
    }

    adist[mm[1],] <- adist[mm[2],] <- adist[,mm[1]] <- adist[,mm[2]] <- Inf
    ws <- c(ws[!(ws == mm[1] | ws == mm[2])], currentNode)
    currentNode <- currentNode + 1
    j <- j + 2
  }
  tree[nnodes] <- nnodes

  return(list(tree, graph))
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

tstAA <- matrix(c(0,7,8,11,13,16,13,17,
                  7,0,5,8,10,13,10,14,
                  8,5,0,5,7,10,7,11,
                  11,8,5,0,8,11,8,12,
                  13,10,7,8,0,5,6,10,
                  16,13,10,11,5,0,9,13,
                  13,10,7,8,6,9,0,8,
                  17,14,11,12,10,13,8,0), 8,8)


A <- matrix(c(0,5,4,7,6,8,
              5,0,7,10,9,11,
              4,7,0,7,6,8,
              7,10,7,0,5,9,
              6,9,6,5,0,8,
              8,11,8,9,8,0), nrow = 6, ncol = 6)


tst <- matrix(c(0,2,8,10,12,2,0,14,16,24,8,14,
                0,4,6,10,16,4,0,6,12,24,6,6,0),
              nrow = 5, ncol = 5)
diag(tst) <- Inf
