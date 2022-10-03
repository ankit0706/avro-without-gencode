package com.example;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

/**
 * In this example we will perform the same serialization and de-serialization process using Avro
 * but this time, we will not generate java code from our schema. Specific Class names are now
 * replaced with GenericRecord, GenericData, GenericDatumWriter, GenericDatumReader and so on.
 * This generic classes help us in the read/write process in an abstract manner. In this project
 * we have not included the Avro Maven plugin whose role was to generate the java code from avro schema.
 * Since we are using the generic classes from Avro, we do not need the specific java code for our schema 
 * @author ankit
 *
 */
public class AvroMain {

    public static void main(String[] args) throws IOException {
        /*
         * We're using Schema class to directly read from the avro schema. In the alternate approach, 
         * we generate java code from the schema and then use the generated java class; hence
         * removing the dependency on schema. But in this approach, our custom java code is directly 
         * using the schema         
         */
        Schema schema = new Schema.Parser().parse(new File("src\\main\\resources\\user.avsc"));
        /*
         * The schema is placed in resources folder which can directly be accessed as shown
         * above. At the time of code execution, the project folder is the root directory
         */

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "India");
        user1.put("favorite_number", 1947);
        // We can leave favorite color null

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "South Africa");
        user2.put("favorite_number", 1992);
        user2.put("favorite_color", "green");

        // ====WRITING=============================================

        // If no absolute or relative path is given, the serialized file is written at root directory
        File serializedFile = new File("users_output.avro");
        /*
         * DatumWriter<User> is replaced by DatumWriter<GenericRecord>; 
         * SpecificDatumWriter<User>(User.class) changes into GenericDatumWriter<GenericRecord>(schema)
         */
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, serializedFile);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();

        // ====READING=============================================
        /*
         * DatumReader<User> is replaced by DatumReader<GenericRecord>
         * DataFileReader<User> changes into DataFileReader<GenericRecord> 
         */
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(serializedFile, datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
        dataFileReader.close();
    }

}
