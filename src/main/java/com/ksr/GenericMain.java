package com.ksr;

import com.ksr.avro.Department;
import com.ksr.avro.Employee;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class GenericMain {
    public static void main(String[] args) throws IOException {

        File employeeData = new File("src/main/avro/employee.avsc");

        //Serializing and deserializing without code generation. Using Schema parsers
        Schema schema = new Schema.Parser().parse(employeeData);
        GenericRecord empl1 = new GenericData.Record(schema);
        //Get the inner object Department
        GenericData.Record dept = new GenericData.Record(schema.getField("department").schema());
        dept.put("id", 99);
        dept.put("name", "Accounts");

        empl1.put("id", 1);
        empl1.put("first", "Nikhil");
        empl1.put("last", "Chinnappa");
        empl1.put("department", dept);

        GenericRecord empl2 = new GenericData.Record(schema);
        empl2.put("id", 2);
        empl2.put("first", "Rick");
        empl2.put("last", "Ross");
        empl2.put("department", dept);

        GenericRecord empl3 = new GenericData.Record(schema);
        empl3.put("id", 3);
        empl3.put("first", "Sunil");
        empl3.put("last", "Gav");
        empl3.put("department", dept);

        File employeeDataNonGen = new File("target/generated-sources/employee_nongen.avro");
        //Serialize
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter1 = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter1.create(schema, employeeDataNonGen);
        dataFileWriter1.append(empl1);
        dataFileWriter1.append(empl2);
        dataFileWriter1.append(empl3);
        dataFileWriter1.close();

        // Deserialize employees from disk
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(employeeDataNonGen, datumReader);
        GenericRecord emp = null;
        while (dataFileReader.hasNext()) {
            emp = dataFileReader.next(emp);
            System.out.println(emp);
        }
    }
}
