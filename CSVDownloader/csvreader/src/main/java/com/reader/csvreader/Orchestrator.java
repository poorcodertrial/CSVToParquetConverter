package com.reader.csvreader;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;

/**
 * Read CSV and write to Parquet
 *
 */
public class Orchestrator

{
	private static final String ACCESS_KEY_ID = "";

	private static final String SECRET_KEY = "";

	private static final String S3_BUCKET_NAME = "";

	public static void main(String[] args) {

		AWSCredentials tempCreds = new BasicAWSCredentials(ACCESS_KEY_ID, SECRET_KEY);

		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(tempCreds)).withRegion(Regions.AP_SOUTHEAST_2)
				.build();
		S3Object object = s3Client.getObject(S3_BUCKET_NAME, "data.zip");
		ZipInputStream zis = new ZipInputStream(object.getObjectContent());
		readTheFileFromS3(zis);
		uploadFilesToS3(s3Client);

	}

	/**
	 * Read the csv files from s3
	 * 
	 * @param zis
	 */
	protected static void readTheFileFromS3(ZipInputStream zis) {
		List<GenericSchema> dataList = new ArrayList<>();
		BufferedReader reader = null;
		GenericSchema genericSchema = null;

		try {
			String line;
			ZipEntry entry = null;
			while ((entry = zis.getNextEntry()) != null) {
				String key = entry.getName().substring(0, entry.getName().indexOf("."));
				reader = new BufferedReader(new InputStreamReader(zis));
				while ((line = reader.readLine()) != null) {
					if (line.contains("ellipsis")) {
						genericSchema = new GenericSchema();
						genericSchema.setCol1(line);
						dataList.add(genericSchema);
					}
				}
				writeParquetFile(dataList, key);
			}			
		} catch (SdkClientException | IOException e) {
			e.printStackTrace();
		}		

	}

	/**
	 * Method to create parquet files in a temp folder
	 * 
	 * @param dataList
	 * @param fileName
	 */
	@SuppressWarnings("rawtypes")
	protected static void writeParquetFile(List<GenericSchema> dataList, String fileName) {

		Schema avroSchema = GenericSchema.getClassSchema();

		Path filePath = new Path("./temp/" + fileName + ".parquet");
		int blockSize = 1024;
		int pageSize = 65535;
		try (AvroParquetWriter parquetWriter = new AvroParquetWriter(filePath, avroSchema, CompressionCodecName.SNAPPY,
				blockSize, pageSize)) {
			for (GenericSchema obj : dataList) {
				parquetWriter.write(obj);
			}
		} catch (java.io.IOException e) {
			System.out.println(String.format("Error writing parquet file %s", e.getMessage()));
			e.printStackTrace();
		}

	}

	/**
	 * Upload the files from temp folder to S3
	 * 
	 * @param s3Client
	 */
	protected static void uploadFilesToS3(AmazonS3 s3Client) {

		try {
			List<File> files = Files.list(Paths.get("./temp")).filter(Files::isRegularFile)
					.filter(path -> path.toString().endsWith(".parquet")).map(java.nio.file.Path::toFile)
					.collect(Collectors.toList());
			files.forEach(file -> {
				PutObjectResult result = s3Client.putObject(S3_BUCKET_NAME, file.getName(), file);
				// Get the Etag to check that the file upload is successful
				System.out.println(result.getETag());
			});
		} catch (SdkClientException | IOException e) {
			e.printStackTrace();
		} finally {
			try {
				// Delete the temp folder
				FileUtils.deleteDirectory(new File("./temp"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
