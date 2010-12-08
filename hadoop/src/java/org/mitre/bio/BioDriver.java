/**
 * Created on March 24, 2009.
 *
 * Copyright 2010- The MITRE Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you  may not 
 * use this file except in compliance with the License. You may obtain a copy of 
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT 
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions andlimitations under
 * the License.
 *
 * $Id$
 */
package org.mitre.bio;

import org.apache.hadoop.util.ProgramDriver;

import org.mitre.bio.mapred.Fasta2SequenceFile;
import org.mitre.bio.mapred.TotalSequenceLength;

import org.mitre.ccv.mapred.CalculateCompositionVectors;
import org.mitre.ccv.mapred.CalculateCosineDistanceMatrix;
import org.mitre.ccv.mapred.CalculateKmerCounts;
import org.mitre.ccv.mapred.CalculateKmerPiValues;
import org.mitre.ccv.mapred.CalculateKmerProbabilities;
import org.mitre.ccv.mapred.CalculateKmerRevisedRelativeEntropy;
import org.mitre.ccv.mapred.CompleteCompositionVectorUtils;
import org.mitre.ccv.mapred.CompleteCompositionVectors;
import org.mitre.ccv.mapred.GenerateFeatureVectors;
import org.mitre.ccv.mapred.InvertKmerProbabilities;
import org.mitre.ccv.mapred.SortKmerRevisedRelativeEntropies;

import org.mitre.ccv.weka.mapred.ClassifyInstances;

/**
 * Driver that runs programs based its given name.
 *
 * <P>A human-readable description of each is also given</P>
 *
 * @author Marc Colosimo
 */
public class BioDriver {

    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver pgd = new ProgramDriver();
        try {
            pgd.addClass("Fasta2SequenceFile", Fasta2SequenceFile.class,
                    "A map/reduce program that parses FASTA formated files into a SequenceFiles.");

            // CCV Main Classes
            pgd.addClass("HCCV", CompleteCompositionVectors.class,
                    "A map/reduce program that that preforms all the steps to produce feature vectors.");
            pgd.addClass("TotalSequenceLength", TotalSequenceLength.class,
                    "A map/reduce program that calculates the length of all the sequences in SequenceFiles.");
            pgd.addClass("CalculateKmerCounts", CalculateKmerCounts.class,
                    "A map/reduce program that calculates the counts of all the k-mers in a range from sequences in SequenceFiles.");
            pgd.addClass("CalculateKmerProbabilities", CalculateKmerProbabilities.class,
                    "A map/reduce program that calculates the frequency of all the k-mer counts in SequenceFiles.");
            pgd.addClass("InvertKmerProbabilities", InvertKmerProbabilities.class,
                    "A map/reduce program that inverts the frequency of all the k-mers frequencies in SequenceFiles, generating a k-mer and its subsequences.");
            pgd.addClass("CalculateKmerPiValues", CalculateKmerPiValues.class,
                    "A map/reduce program that calculates the pi-values for inverted k-mer frequencies in SequenceFiles.");
            pgd.addClass("CalculateCompositionVectors", CalculateCompositionVectors.class,
                    "A map/reduce program that calculates the composition vectors for sequecnes in SequenceFiles.");
            pgd.addClass("CalculateKmerRevisedRelativeEntropy", CalculateKmerRevisedRelativeEntropy.class,
                    "A map/reduce program that calculates the revised relative entropy from composition vectors and k-mer pi-values");
            pgd.addClass("SortKmerRevisedRelativeEntropies", SortKmerRevisedRelativeEntropies.class,
                    "A map/reduce program that sorts the revised relative entropy from largest to smallest");
            pgd.addClass("GenerateFeatureVectors", GenerateFeatureVectors.class,
                    "A map/reduce program that generates feature vectors from the composition vectors and k-mer list");
            pgd.addClass("CompleteCompositionVectorUtils", CompleteCompositionVectorUtils.class,
                    "Utility methods. Use with caution!");

            pgd.addClass("CalculateCosineDistanceMatrix", CalculateCosineDistanceMatrix.class,
                    "Generates a Cosine Distance Matrix using the generated FeatureVectors");
            
            // Classifier Classes
            pgd.addClass("ClassifyInstances", ClassifyInstances.class,
                    "A map/reduce program that classifies samples from SequenceFiles using a pre-built model");

            pgd.driver(argv);
            // Success
            exitCode = 0;
        } catch (Throwable e) {
            //e.printStackTrace();
            System.err.println(e.getMessage());
        }

        System.exit(exitCode);
    }
}
