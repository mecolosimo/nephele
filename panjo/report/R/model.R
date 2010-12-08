## all in bytes. ##

# msgs   size 
# P^2,   (N/P)*8  
# P^2,   1*8 
# P,     2*4
# P,     1*8 
# P,     (N/P)

# The alpha + beta model says that the performance is dependent upon the
# parameters alpha and beta where alpha represents the latency and beta
# represents the bandwidth.
# Jaquard parameters:

# alpha = 4.5 usec * (1 s)/(1.0 × 10e6 usec) = 4.5e-6
# beta  = 620 MB/s * (1024^2 B)/(1 MB) = 650117120

computeBytes <- function(N, P, alpha = 4.5e-6, beta = 650117120) {
    nmsgs <- P^2 + P^2 + P + P + P
    ts <- (N/P)*8 + 8 + 8 + 8 + (N/P)*8

    return(N*nmsgs*(alpha + (ts*(1/beta))))
}
