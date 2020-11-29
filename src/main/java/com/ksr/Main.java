package com.ksr;

import com.ksr.avro.Department;
import com.ksr.avro.Employee;

public class Main {
    public static void main(String[] args){

        Department dept = new Department();
        dept.setId(99);
        dept.setName("Accounts");

        Employee emp1 = new Employee();
        emp1.setId(1);
        emp1.setFirst("Nikhil");
        emp1.setLast("Chinnappa");
        emp1.setDepartment(dept);

        // Alternate constructor
        Employee emp2 = new Employee(2, "Rick", "Ross", "RR", 27, dept);

        //Construct via builder
        Employee emp3 = Employee.newBuilder()
                .setId(3)
                .setFirst("Sunil")
                .setLast("Gav")
                .setAlias("gav")
                .setAge(55)
                .setDepartment(dept)
                .build();
    }


}
