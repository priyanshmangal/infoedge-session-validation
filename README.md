# Session Validation
There is visitor_id, session_id, timestamp in a csv.
Ideally, there should be at least 30 minutes difference in any two sessions by any visitor.
But we have some cases where time diff is less.
Problems:
1. Find out all the unique visitors which have this anomaly
2. Find out such extra sessions
3. Create a new field which should have the intended session id - first session id should persist as long as there is no 30 minute inactivity window

# Data Path
 https://drive.google.com/open?id=1Vq0lyB0u1gRt5twc8jHWCY9BAIwV4yFz
 
Please check SessionValidationJob class for dataframe implementation for session validation
and check SessionRDDValidation class for RDD implementation for session validation


# Resources
Input Output File And Rule Set Json file is maintained in Data Folder.

# Key Features
 1) This is analytics.conf driven job where you can manage your input output and rule set json file path accordingly.
 2) In this code we can update getSessionAnalysisDF function as per requirement in future.
 3) This is complete imputable code.
 4) Logger is implemented in job to track as per requirement.
 5) Best Practices Followed  - Unit Test, TDD and Doc String for all functions. 
 
# Future Scope Of Improvement
1) Better Data Validation Function covering all edge case.
2) Architecture design of code can be improved.
