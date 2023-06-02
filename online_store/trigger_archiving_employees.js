exports = function(changeEvent) {
    const collection = context.services.get("NoSQL-TP").db("tp").collection("employees_archive");
    const oldData = (changeEvent.operationType === 'insert') ? { EMPLOYEES: [] } : changeEvent.fullDocumentBeforeChange
    const newData = (changeEvent.operationType === 'delete') ? { EMPLOYEES: [] } : changeEvent.fullDocument
    
    
    const new_ids = newData.EMPLOYEES.map(employee => employee.EMPLOYEE_ID)
    const old_ids = oldData.EMPLOYEES.map(employee => employee.EMPLOYEE_ID)
    const new_employees = new_ids.filter(x => !old_ids.includes(x))
    let old_employees = old_ids.filter(x => !new_ids.includes(x))
    
    // Insert
    if(new_employees.length > 0) {
      new_employees.forEach(employee => {
        console.log(JSON.stringify(employee))
        const new_employee = newData.EMPLOYEES.filter(x => employee === x.EMPLOYEE_ID)[0]
        
        collection.insertOne({
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
          "NEW_EMPLOYEE_ID": new_employee.EMPLOYEE_ID,
          "NEW_FIRST_NAME": new_employee.FIRST_NAME,
          "NEW_MIDDLE_NAME": new_employee.MIDDLE_NAME,
          "NEW_LAST_NAME": new_employee.LAST_NAME,
          "NEW_DATE_OF_BIRTH": new_employee.DATE_OF_BIRTH,
          "NEW_DEPARTMENT_ID": changeEvent.fullDocument._id,
          "NEW_HIRE_DATE": new_employee.HIRE_DATE,
          "NEW_SALARY": new_employee.SALARY,
          "NEW_PHONE_NUMBER": new_employee.PHONE_NUMBER,
          "NEW_EMAIL": new_employee.EMAIL,
          "NEW_SSN_NUMBER": new_employee.SSN_NUMBER,
          "NEW_MANAGER_ID": new_employee.E_MANAGER_ID
        })
      })
    }
    

    // Delete
    if(changeEvent.operationType === 'delete')
      old_employees = old_ids
      
    if(old_employees.length > 0) {
      console.log(JSON.stringify(oldData))
      old_employees.forEach(employee => {
        
        const old_employee = oldData.EMPLOYEES.filter(x => employee === x.EMPLOYEE_ID)[0]
        
        collection.insertOne({
          "EVENT_DATE": new Date().toJSON(), // Get curr time fullDocumentBeforeChange
          "EVENT_TYPE": "DELETE",
          "USER_NAME": "LOJA", // Get user name
          "OLD_EMPLOYEE_ID": old_employee.EMPLOYEE_ID,
          "OLD_FIRST_NAME": old_employee.FIRST_NAME,
          "OLD_MIDDLE_NAME": old_employee.MIDDLE_NAME,
          "OLD_LAST_NAME": old_employee.LAST_NAME,
          "OLD_DATE_OF_BIRTH": old_employee.DATE_OF_BIRTH,
          "OLD_DEPARTMENT_ID": changeEvent.fullDocumentBeforeChange._id,
          "OLD_HIRE_DATE": old_employee.HIRE_DATE,
          "OLD_SALARY": old_employee.SALARY,
          "OLD_PHONE_NUMBER": old_employee.PHONE_NUMBER,
          "OLD_EMAIL": old_employee.EMAIL,
          "OLD_SSN_NUMBER": old_employee.SSN_NUMBER,
          "OLD_MANAGER_ID": old_employee.E_MANAGER_ID,
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
    }
    
    // Update/Replace
    if (JSON.stringify(new_ids) === JSON.stringify(old_ids)) {
      const old_stringified = oldData.EMPLOYEES.map(employee => JSON.stringify(employee))
      const updated_employees = newData.EMPLOYEES.filter(employee => !old_stringified.includes(JSON.stringify(employee)))
      
      updated_employees.forEach(new_employee => {
        const old_employee = oldData.EMPLOYEES.filter(e => e.EMPLOYEE_ID === new_employee.EMPLOYEE_ID)[0]
        collection.insertOne({
          "EVENT_DATE": new Date().toJSON(), // Get curr time fullDocumentBeforeChange
          "EVENT_TYPE": "UPDATE",
          "USER_NAME": "LOJA", // Get user name
          "OLD_EMPLOYEE_ID": old_employee.EMPLOYEE_ID,
          "OLD_FIRST_NAME": old_employee.FIRST_NAME,
          "OLD_MIDDLE_NAME": old_employee.MIDDLE_NAME,
          "OLD_LAST_NAME": old_employee.LAST_NAME,
          "OLD_DATE_OF_BIRTH": old_employee.DATE_OF_BIRTH,
          "OLD_DEPARTMENT_ID": changeEvent.fullDocumentBeforeChange._id,
          "OLD_HIRE_DATE": old_employee.HIRE_DATE,
          "OLD_SALARY": old_employee.SALARY,
          "OLD_PHONE_NUMBER": old_employee.PHONE_NUMBER,
          "OLD_EMAIL": old_employee.EMAIL,
          "OLD_SSN_NUMBER": old_employee.SSN_NUMBER,
          "OLD_MANAGER_ID": old_employee.E_MANAGER_ID,
          "NEW_EMPLOYEE_ID": new_employee.EMPLOYEE_ID,
          "NEW_FIRST_NAME": new_employee.FIRST_NAME,
          "NEW_MIDDLE_NAME": new_employee.MIDDLE_NAME,
          "NEW_LAST_NAME": new_employee.LAST_NAME,
          "NEW_DATE_OF_BIRTH": new_employee.DATE_OF_BIRTH,
          "NEW_DEPARTMENT_ID": changeEvent.fullDocument._id,
          "NEW_HIRE_DATE": new_employee.HIRE_DATE,
          "NEW_SALARY": new_employee.SALARY,
          "NEW_PHONE_NUMBER": new_employee.PHONE_NUMBER,
          "NEW_EMAIL": new_employee.EMAIL,
          "NEW_SSN_NUMBER": new_employee.SSN_NUMBER,
          "NEW_MANAGER_ID": new_employee.E_MANAGER_ID
        })
      })
    }
  };
  