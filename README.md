# nephele
The classic genotyping approach has been based on phylogenetic analysis, starting with a multiple sequence alignment. Genotypes are then established by expert examination of phylogenetic trees. However, such methods are suboptimal for a rapidly growing dataset, because they require significant human effort, and because they increase in computational complexity quickly with the number of sequences. This project uses a method for genotyping that does not depend on multiple sequence alignment. It uses the complete composition vector algorithm to represent each sequence in the dataset as a vector derived from its constituent k-mers, and affinity propagation clustering to group the sequences into genotypes based on a distance measure over the vectors. Our methods produce results that correlate well with expert-defined clades or genotypes, at a fraction of the computational cost of traditional phylogenetic methods.


[Nephele: Genotyping via Complete Composition Vectors and MapReduce](http://www.scfbm.org/content/6/1/13/abstract) in Source Code for Biology and Medicine


This was automatically exported from Google Code [Nephele Project](code.google.com/p/nephele) on 29 March 2015. 
