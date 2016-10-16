package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;

public class SparkDriver {
	
	public static void main(String[] args) {

		String inputPath;
		
		inputPath=args[0];


	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab8");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Create a Spark SQL Context object
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		
		// Read the content of the input file
		// Each element/string of the logRDD corresponds to one line of the input file  
		JavaRDD<String> logRDD = sc.textFile(inputPath).filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String string) throws Exception {
				return !string.startsWith("Id"); //filter the header
			}
		});
		
        //PART 1: READ AND FILTER THE DATASET INTO A DATAFRAME
		// Select the columns in the input file
		JavaRDD<AmazonReview> review = logRDD.map(new Function<String, AmazonReview>() {
			public AmazonReview call(String line) throws Exception {
//			    String[] parts = line.split(",");
			    String[] parts = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); //ignore commas inside quotes

			    AmazonReview p = new AmazonReview();
			    p.setStars(Integer.parseInt(parts[6]));
                p.setHelpDenom((int) Double.parseDouble(parts[5]));
			    p.setHelp(Double.parseDouble(parts[4])/p.getHelpDenom());
                p.setIsHelpfulDouble((p.getHelp() > 0.9) ? 1 : 0);
                p.setLenText(parts[8].length());

                // SOLUTION TO EX.1-2:
                //p.setFeatures(Vectors.dense(p.getLenText()));

//              SOLUTION TO EX.3:

                p.setFeatures(Vectors.dense(p.getStars(),
                        p.getLenText(),
                        parts[9].length(),
                        parts[8].split("!").length,
                        parts[9].split("!").length));
			    return p;
			}
		    });
		DataFrame schemaReviews = sqlContext.createDataFrame(review, AmazonReview.class);

		schemaReviews.show(5);

        // Filter out the reviews with helpfulness denominator < 1 (avoid NaN)

        schemaReviews.registerTempTable("reviews");
        schemaReviews = sqlContext.sql("SELECT * FROM reviews WHERE helpDenom > 0");

        //// Same filter as in query above:
        //schemaReviews = schemaReviews.filter(schemaReviews.col("helpDenom").gt(0));

        schemaReviews.show(5);



        //PART 2: CREATE THE PREDICTION PIPELINE

        // For creating a decision tree a label attribute
        // with specific metadata is needed
        // The StringIndexer Estimator is used to achieve this operation
        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol("isHelpfulDouble")
                .setOutputCol("label")
                .fit(schemaReviews);


        // Create a DecisionTreeClassifier object.
        // DecisionTreeClassifier is an Estimator that is used to
        // create a classification model based on decision trees
		DecisionTreeClassifier dt = new DecisionTreeClassifier();
//		    .setLabelCol("label")
//		    .setFeaturesCol("features");



        // Convert indexed labels back to original labels.
        // The content of the prediction attribute is the index of the
        // predicted class
        // The original name of the predicted class is stored in
        // the predictedLabel attribute
        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());		// Chain indexers and tree in a Pipeline

		Pipeline pipeline = new Pipeline()
		    .setStages(new PipelineStage[]{labelIndexer, dt, labelConverter});

        // Split the data into training and test sets (30% held out for testing)
        DataFrame[] splits = schemaReviews.randomSplit(new double[]{0.7, 0.3});
        DataFrame trainingData = splits[0];
        DataFrame testData = splits[1];

		// Train model.  This also runs the indexers.
		PipelineModel model = pipeline.fit(trainingData);

		// Make predictions.
		DataFrame predictions = model.transform(testData);

		// Select example rows to display.
		//predictions.select("prediction", "label", "features", "isHelpfulDouble", "predictedLabel").show(5);
        predictions.show(5);

        MulticlassMetrics metrics = new MulticlassMetrics(predictions.select("prediction", "label"));

        // Confusion matrix
        Matrix confusion = metrics.confusionMatrix();
        System.out.println("Confusion matrix: \n" + confusion);

        double precision = metrics.precision();
		System.out.println("Precision = " + precision);

		DecisionTreeClassificationModel treeModel = (DecisionTreeClassificationModel) (model.stages()[1]);
		System.out.println("Learned classification tree model:\n" + treeModel.toDebugString());



            
        // Close the Spark context
		sc.close();
	}
}
