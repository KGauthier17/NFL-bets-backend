// src/main/java/com/example/nflbets/service/PredictionService.java
package com.example.NFL_bets_backend.service;

import com.example.NFL_bets_backend.model.PredictionRequest;
import org.springframework.stereotype.Service;
import weka.classifiers.Classifier;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;

import jakarta.annotation.PostConstruct;
import java.io.File;

@Service
public class PredictionService {

    private Classifier classifier;

    @PostConstruct
    public void init() {
        try {
            // Load the trained model
            classifier = (Classifier) weka.core.SerializationHelper.read("path/to/your/model.model");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public double predict(PredictionRequest request) {
        try {
            // Load the structure of your dataset (ARFF file)
            Instances dataset = DataSource.read("path/to/your/dataset.arff");
            dataset.setClassIndex(dataset.numAttributes() - 1);

            // Create a new instance with the input data
            double[] instanceValue1 = new double[dataset.numAttributes()];
            instanceValue1[0] = request.getFeature1();  // Set feature1 value
            instanceValue1[1] = request.getFeature2();  // Set feature2 value
            // Set other features...

            // Add the instance to the dataset
            dataset.add(dataset.instance(0).copy(instanceValue1));

            // Get the last instance (the one we just added)
            Instances testset = new Instances(dataset, 0, 1);

            // Predict the class value
            double prediction = classifier.classifyInstance(testset.instance(0));
            return prediction;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;  // Return a default value in case of an error
    }
}
