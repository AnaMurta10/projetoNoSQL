exports = function(changeEvent) {
    /*
      A Database Trigger will always call a function with a changeEvent.
      Documentation on ChangeEvents: https://docs.mongodb.com/manual/reference/change-events/
  
      Access the _id of the changed document:
      const docId = changeEvent.documentKey._id;
  
      Access the latest version of the changed document
      (with Full Document enabled for Insert, Update, and Replace operations):
      const fullDocument = changeEvent.fullDocument;
  
      const updateDescription = changeEvent.updateDescription;
  
      See which fields were changed (if any):
      if (updateDescription) {
        const updatedFields = updateDescription.updatedFields; // A document containing updated fields
      }
  
      See which fields were removed (if any):
      if (updateDescription) {
        const removedFields = updateDescription.removedFields; // An array of removed fields
      }
  
      Functions run by Triggers are run as System users and have full access to Services, Functions, and MongoDB Data.
  
      Access a mongodb service:
      const collection = context.services.get(<SERVICE_NAME>).db("db_name").collection("coll_name");
      const doc = collection.findOne({ name: "mongodb" });
  
      Note: In Atlas Triggers, the service name is defaulted to the cluster name.
  
      Call other named functions if they are defined in your application:
      const result = context.functions.execute("function_name", arg1, arg2);
  
      Access the default http client and execute a GET request:
      const response = context.http.get({ url: <URL> })
  
      Learn more about http client here: https://www.mongodb.com/docs/atlas/app-services/functions/context/#std-label-context-http
    */
    const collection = context.services.get("NoSQL-TP").db("tp").collection("employees_archive");
    
    const new_ids = changeEvent.fullDocument.EMPLOYEES.map(employee => employee.EMPLOYEE_ID)
    const old_ids = changeEvent.fullDocumentBeforeChange.EMPLOYEES.map(employee => employee.EMPLOYEE_ID)
    const new_employees = new_ids.filter(x => !old_ids.includes(x))
    const old_employees = old_ids.filter(x => !new_ids.includes(x))
    const new_employee = changeEvent.fullDocument.EMPLOYEES.filter(x => x.EMPLOYEE_ID === diff);
    console.log(JSON.stringify(new_employee))
    
    if(new_employees.length > 0) {
      new_employees.foreach(employee => {
        const new_employee = changeEvent.fullDocument.EMPLOYEES.filter(x => employee.EMPLOYEE_ID === x.EMPLOYEE_ID)[0]
        
        collection.insert({
          "EVENT_DATE": new Date().toJSON(), // Get curr time
          "EVENT_TYPE": "INSERT",
          "USER_NAME": "LOJA", // Get user name
          "OLD_EMPLOYEE_ID": null,
          "OLD_FIRST_NAME": null,
          "OLD_MIDDLE_NAME": null,
          "OLD_LAST_NAME": null,
          "OLD_DATE_OF_BIRTH": null,
          "OLD_DEPARTMENT_ID": null,
          "OLD_HIRE_DATE": null,
          "OLD_SALARY": null,
          "OLD_PHONE_NUMBER": null,
          "OLD_EMAIL": null,
          "OLD_SSN_NUMBER": null,
          "OLD_MANAGER_ID": null,
          "NEW_EMPLOYEE_ID": new_employee._id,
          "NEW_FIRST_NAME": new_employee.first_name,
          "NEW_MIDDLE_NAME": new_employee.middle_name,
          "NEW_LAST_NAME": new_employee.last_name,
          "NEW_DATE_OF_BIRTH": new_employee.date_of_birth,
          "NEW_DEPARTMENT_ID": new_employee.department_id,
          "NEW_HIRE_DATE": new_employee.hire_date,
          "NEW_SALARY": new_employee.salary,
          "NEW_PHONE_NUMBER": new_employee.phone_number,
          "NEW_EMAIL": new_employee.email,
          "NEW_SSN_NUMBER": new_employee.ssn_number,
          "NEW_MANAGER_ID": new_employee.manager_id
        })
      })
    } else if(old_employees.length > 0) {
      old_employees.foreach(employee => {
        const old_employee = changeEvent.fullDocumentBeforeChange.EMPLOYEES.filter(x => employee.EMPLOYEE_ID === x.EMPLOYEE_ID)[0]
        
        collection.insert({
          "EVENT_DATE": new Date().toJSON(), // Get curr time fullDocumentBeforeChange
          "EVENT_TYPE": "DELETE",
          "USER_NAME": "LOJA", // Get user name
          "OLD_EMPLOYEE_ID": old_employee._id,
          "OLD_FIRST_NAME": old_employee.first_name,
          "OLD_MIDDLE_NAME": old_employee.middle_name,
          "OLD_LAST_NAME": old_employee.last_name,
          "OLD_DATE_OF_BIRTH": old_employee.date_of_birth,
          "OLD_DEPARTMENT_ID": old_employee.department_id,
          "OLD_HIRE_DATE": old_employee.hire_date,
          "OLD_SALARY": old_employee.salary,
          "OLD_PHONE_NUMBER": old_employee.phone_number,
          "OLD_EMAIL": old_employee.email,
          "OLD_SSN_NUMBER": null,
          "OLD_MANAGER_ID": null,
          "NEW_EMPLOYEE_ID": null,
          "NEW_FIRST_NAME": null,
          "NEW_MIDDLE_NAME": null,
          "NEW_LAST_NAME": null,
          "NEW_DATE_OF_BIRTH": null,
          "NEW_DEPARTMENT_ID": null,
          "NEW_HIRE_DATE": null,
          "NEW_SALARY": null,
          "NEW_PHONE_NUMBER": null,
          "NEW_EMAIL": null,
          "NEW_SSN_NUMBER": null,
          "NEW_MANAGER_ID": null
        })
      })
    } else if (JSON.stringify(new_ids) === JSON.stringify(old_ids)) {
      const old_stringified = changeEvent.fullDocumentBeforeChange.EMPLOYEES.map(employee => JSON.stringify(employee))
      const updated_employees = changeEvent.fullDocument.EMPLOYEES.filter(employee => !old_stringified.includes(JSON.stringify(employee)))
      
      updated_employees.foreach(employee => {
        const old_employee = changeEvent.fullDocumentBeforeChange.EMPLOYEES.filter(e => e.EMPLOYEE_ID === employee.EMPLOYEE_ID)[0]
        collection.insert({
          "EVENT_DATE": new Date().toJSON(), // Get curr time fullDocumentBeforeChange
          "EVENT_TYPE": "UPDATE",
          "USER_NAME": "LOJA", // Get user name
          "OLD_EMPLOYEE_ID": old_employee._id,
          "OLD_FIRST_NAME": old_employee.first_name,
          "OLD_MIDDLE_NAME": old_employee.middle_name,
          "OLD_LAST_NAME": old_employee.last_name,
          "OLD_DATE_OF_BIRTH": old_employee.date_of_birth,
          "OLD_DEPARTMENT_ID": old_employee.department_id,
          "OLD_HIRE_DATE": old_employee.hire_date,
          "OLD_SALARY": old_employee.salary,
          "OLD_PHONE_NUMBER": old_employee.phone_number,
          "OLD_EMAIL": old_employee.email,
          "OLD_SSN_NUMBER": old_employee.ssn_number,
          "OLD_MANAGER_ID": old_employee.manager_id,
          "NEW_EMPLOYEE_ID": employee._id,
          "NEW_FIRST_NAME": employee.first_name,
          "NEW_MIDDLE_NAME": employee.middle_name,
          "NEW_LAST_NAME": employee.last_name,
          "NEW_DATE_OF_BIRTH": employee.date_of_birth,
          "NEW_DEPARTMENT_ID": employee.department_id,
          "NEW_HIRE_DATE": employee.hire_date,
          "NEW_SALARY": employee.salary,
          "NEW_PHONE_NUMBER": employee.phone_number,
          "NEW_EMAIL": employee.email,
          "NEW_SSN_NUMBER": employee.ssn_number,
          "NEW_MANAGER_ID": employee.manager_id
        })
      })
    }
  };
  