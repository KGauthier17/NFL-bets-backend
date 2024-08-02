package com.example.NFL_bets_backend;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import weka.classifiers.Classifier;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Paths;

public class PredictionHandler implements RequestHandler<PredictionRequest, PredictionResponse> {

    private static final String BUCKET_NAME = "your-s3-bucket-name";
    private static final String MODEL_KEY = "path/to/your/model.joblib";
    private static final String DATASET_PATH = "local/path/to/your/dataset.arff";
    private static final String API_URL = "https://api.example.com/betting-lines";

    private Classifier classifier;
    private Instances dataset;

    public PredictionHandler() {
        init();
    }

    private void init() {
        try {
            // Download the model file from S3
            S3Client s3 = S3Client.builder()
                    .region(Region.US_EAST_1)
                    .credentialsProvider(ProfileCredentialsProvider.create())
                    .build();

            File modelFile = File.createTempFile("model", ".joblib");
            s3.getObject(GetObjectRequest.builder()
                            .bucket(BUCKET_NAME)
                            .key(MODEL_KEY)
                            .build(),
                    Paths.get(modelFile.getAbsolutePath()));

            // Load the model
            classifier = (Classifier) weka.core.SerializationHelper.read(modelFile.getAbsolutePath());

            // Load dataset structure
            dataset = DataSource.read(DATASET_PATH);
            dataset.setClassIndex(dataset.numAttributes() - 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String fetchDataFromAPI() throws Exception {
        URL url = new URL(API_URL);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String inputLine;
        StringBuilder content = new StringBuilder();
        while ((inputLine = in.readLine()) != null) {
            content.append(inputLine);
        }
        in.close();
        conn.disconnect();
        return content.toString();
    }

    @Override
    public PredictionResponse handleRequest(PredictionRequest request, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Received request: " + request.toString());

        try {
            // Fetch data from API
            String apiResponse = fetchDataFromAPI();
            // Process API response and create a PredictionRequest

            // Create a new instance for prediction
            Instance instance = new DenseInstance(dataset.numAttributes());
            instance.setDataset(dataset);
            instance.setValue(0, request.getFeature1());
            instance.setValue(1, request.getFeature2());
            // Set other feature values...

            // Make prediction
            double prediction = classifier.classifyInstance(instance);
            return new PredictionResponse(prediction);
        } catch (Exception e) {
            logger.log("Error during prediction: " + e.getMessage());
        }
        return new PredictionResponse(-1);  // Return a default value in case of an error
    }
}
