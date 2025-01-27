import os
import streamlit as st
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain_core.messages import HumanMessage
from langchain_community.utilities import SQLDatabase
import pyodbc
import pandas as pd

# Load environment variables
load_dotenv()

# Initialize API keys and database credentials
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

# Initialize the language model
llm = ChatOpenAI(
    api_key=OPENAI_API_KEY,
    model="gpt-4o-mini",  # Fixed model name
    temperature=0.0  # Setting temperature to 0 for more precise SQL generation
)

def get_database_schema():
    """Return the hardcoded database schema"""
    schema = """
General Table name: DynamicsShortlisted.dbo.account
Description: 
Columns: 
  accountid (Primary Key) Description : "ID of account. Account is the organization willing to make a purchase."
  address1_line1 (String) Description : "Account's address line one"
  address1_line2 (String) Description : "Account's Address line two"
  address1_postalcode (String) Description : "Account's postal code"
  address2_country (String) Description : "Account's country"
  address2_stateorprovince (String) Description : "Account's state or province name"
  cr93b_accounttype (Integer) Description : "Type of account"
  createdby (String) Description : "ID of the user who created the account"
  createdon (Date) Description : "Date on which the account was created"
  emailaddress1 (String) Description : "Account's email address"
  new_activeinactive (Boolean) Description : "Account's active or inactive status"
  new_outreach (String) Description : "The method of account's signing up, such as Outbound, Referral, etc"
  new_totallicenseaddons (Integer) Description : "License addons bought by account"
  numberofemployees (Integer) Description : "The number of employees account organization has"
  originatingleadid (Foreign key to DynamicsShortlisted.dbo.lead.leadid) Description : "The lead through which this account was created"
  ownerid (String) Description : "Owner or the creator of this account in the system"
  owninguser (String) Description : "The current owner of the account"
  primarycontactid (Foreign key to DynamicsShortlisted.dbo.contact.contactid) Description : "Primary contact person for the account"
  revenue (Decimal) Description : "Telephone number of the account"
  telephone1 (String) Description : "Telephone number of the account"
  versionnumber (Integer) Description : "Account's version number"
  websiteurl (String) Description : "Website URL of the account's organization"
  xt_accountproduct (Integer) Description : "Product ID in which the account is interested in"
  xt_accountrecordtype (Integer) Description : "Record type of account"
  xt_accountsource (Integer) Description : "Source of account"
  xt_adcampaign (String) Description : "The name of the add campaign which led the account's lead signed up"
  xt_additionalclientlicenses (Decimal) Description : "Number of additional client licenses requested"
  xt_additionaldevserverlicenses (Decimal) Description : "Number of additional developer server licenses requested"
  xt_additionalprodserverlicenses (Decimal) Description : "Number of additional production server licenses requested"
  xt_adgroup (String) Description : "The add group in which the add campaign was run"
  xt_attribution (Integer) Description : ""
  xt_clientlicenses (Decimal) Description : "Total number of client licenses"
  xt_comments (String) Description : "Comments by sales person"
  xt_country (String) Description : "Country of the account"
  xt_devserverlicenses (Decimal) Description : "Total number of developer licenses"
  xt_enddate (Date) Description : "Expiration date of the license"
  xt_industry (Integer) Description : "Industry of the account"
  xt_iscustomer (Integer) Description : "Is customer, can have a value of 206220000 if it is a customer or 206220001 if it is a partner or opportunity"
  xt_isdeleted (Boolean) Description : "Is the account deleted"
  xt_keywords (String) Description : "Key words used in add campaigns"
  xt_landingpage (String) Description : "Landing page of the add campaign"
  xt_lastactivitydate (Date) Description : "Last activity on account's information"
  xt_lastmodifieddate (Date) Description : "Last modified date of account's information"
  xt_primarycontact (String) Description : "Name of the primary contact person for this account"
  xt_productname (String) Description : "Name and tier of the product bought by the account"
  xt_productnameis (String) Description : "Name of the product bought by the account"
  xt_purchasetype (Integer) Description : "Purchase type of the account"
  xt_rating (Integer) Description : "Account rating"
  xt_sendrenewalinvoicedate (Date) Description : "License renewal alert date"
  xt_serverlicenses (Decimal) Description : "Total number of server licenses"
  xt_startdate (Date) Description : "License start date"
  xt_type (Integer) Description : "Type of account"

General Table name: DynamicsShortlisted.dbo.annotation
Description: 
Columns: 
  annotationid (Primary Key) Description : "ID of the annotation (note) object"
  createdby (String) Description : "Sales Person/Presales engineer's ID who created this note"
  createdbyname (String) Description : "Name of the Sales person/Presales engineer who created this note"
  createdon (Date) Description : "Date on which this note was created"
  documentbody (String) Description : "If the note has a document, this field contains the body of the document"
  filename (String) Description : "Name of the file added in the note"
  filesize (Integer) Description : "Size of the file added in the note"
  isdocument (Boolean) Description : "true if the note is a document"
  mimetype (String) Description : ""
  modifiedon (Date) Description : "Modified date of the note"
  notetext (String) Description : "Text in the note"
  objectid (Foreign key to multiple tables) Description : "Object on which this note was created on. It can be an email, lead, phone call, contact, account, opportunity, appointment, or task"
  overriddencreatedon (Date) Description : "If created on is overridden by the system, this field will contain its date of override"
  ownerid (String) Description : "The Sales person/Presales Engineer's ID who created this note"
  owneridname (String) Description : "Name of the Sales person/Presales engineer who created this note"
  owninguser (String) Description : "Name of the current Sales person/Presales engineer who owns this note"
  subject (String) Description : "Subject of the note"

General Table name: DynamicsShortlisted.dbo.appointment
Description: 
Columns: 
  activityid (Primary Key) Description : "Appointment ID"
  actualend (Date) Description : "Actual end datetime of the appointment"
  createdby (String) Description : "Sales person/Presales engineer who created the appointment"
  createdon (Date) Description : "Date time on which the appointment was created"
  description (String) Description : "Description of the appointment"
  globalobjectid (String) Description : "Global object ID of the appointment"
  location (String) Description : "Location or platform of the appointment"
  ownerid (String) Description : "Sales person/Presales engineer who is assigned the appointment"
  owninguser (String) Description : "Sales person/Presales engineer who is assigned the appointment"
  regardingobjectid (Foreign key to multiple tables) Description : "Account, lead or item for which the appointment is created"
  scheduleddurationminutes (Integer) Description : "Scheduled duration in minutes"
  scheduledend (Date) Description : "Scheduled end time of the appointment"
  scheduledstart (Date) Description : "Scheduled start time of the appointment"
  subject (String) Description : "Subject or objective of the appointment"
  xt_addteamsmeetinglink (Boolean) Description : "Added teams meeting link or not"
  xt_recordinglink (String) Description : "Recording link of the meeting"
  xt_teamsmeetingurl (String) Description : "Microsoft teams meeting URL"

General Table name: DynamicsShortlisted.dbo.contact
Description: 
Columns: 
  address1_city (String) Description : "Contact person's city"
  address1_line1 (String) Description : "Contact person's address"
  contactid (Primary Key) Description : "ID of the contact person"
  createdby (String) Description : "Sales person/Presales engineer's ID who created the contact person in system"
  createdon (Date) Description : "Date time on which the contact record was created in the system"
  description (String) Description : "Description of the contact person, or notes from the contact person"
  emailaddress1 (String) Description : "Email address of the contact person"
  firstname (String) Description : "First name of the contact person"
  jobtitle (String) Description : "Job title of the contact person"
  lastname (String) Description : "Last name of the contact person"
  leadsourcecode (Integer) Description : "Code of the source of this Contact person"
  mobilephone (String) Description : "Mobile number of the contact person"
  ownerid (String) Description : "Sales person/Presales engineer's ID who created the contact person in system"
  owninguser (String) Description : "Sales person/Presales engineer's ID who created the contact person in system"
  parent_contactid (Foreign key to DynamicsShortlisted.dbo.contact.contactid) Description : "ID of the contact person who refferred this contact person"
  parentcustomerid (Foreign key to multiple tables) Description : "ID of the customer who's contact person this is"
  telephone1 (String) Description : "Telephone number of the contact person"
  xt_adgroup (String) Description : "Add group of the add due to which this contact person signed up"
  xt_attribution (Integer) Description : ""
  xt_comments (String) Description : "Comments by the sales person"
  xt_contactrecordtype (Integer) Description : ""
  xt_contactsource (Integer) Description : "Source from where this contact person originated"
  xt_doyouwantthe (Integer) Description : ""
  xt_hasoptedoutofemail (Boolean) Description : "If the contact has opted out from sending marketing emails, this field will be 1"
  xt_industry (Integer) Description : "Industry of the contact person"
  xt_inferredcity (String) Description : "City of the contact person"
  xt_inferredcountry (String) Description : "Country of the contact person"
  xt_isemailbounced (Boolean) Description : "True if emails sent to the email address of the contact person have bounced"
  xt_keywords (String) Description : "Key words used in add to attract this contact"
  xt_landingpage (String) Description : "Landing page URL of the add campaign"
  xt_leadsource (Integer) Description : "Source of the contact person's lead"
  xt_licensekey (String) Description : "License key provided to the contact person for trial or demo"
  xt_marketingsuspended (Boolean) Description : "If marketing to this customer has been suspended or not"
  xt_notes (String) Description : "Notes from Sales engineer about the contact person"
  xt_otherphone (String) Description : "Any other phone number to reach to this contact person"
  xt_product (String) Description : "Product and tier in which the contact person is interested in"
  xt_productname (String) Description : "Product and tier in which the contact person is interested in"
  xt_productnameis (String) Description : "Product name in which the contact person is interested in"
  xt_referringsite (String) Description : "The site from which the customer navigated to Astera's website"
  xt_referrred_employee_size (Integer) Description : "Employee size of the contact person's organization"
  xt_type (Integer) Description : "Type of contact person"
  xt_unsubscribed (Boolean) Description : "True if contact person has unsubscribed to any sort of communication"

General Table name: DynamicsShortlisted.dbo.email
Description: 
Columns: 
  activityid (Primary Key) Description : "ID of the email"
  actualend (Date) Description : "End time of the email"
  correlatedactivityid (Foreign key to DynamicsShortlisted.dbo.email.activityid) Description : "Activity due to which this email was sent, for example it can be a marketing email or a follow-up email"
  createdby (String) Description : "Sales Person/Presales Engineer who created this email"
  createdon (Date) Description : "Date time on which the email was created on"
  description (String) Description : "Description of the email"
  emailsender (Foreign key to multiple tables) Description : "ID of the sender of the email. It can be a lead, contact or an account"
  lastopenedtime (Date) Description : "Opened date time of the email"
  modifiedon (Date) Description : "Modified date time of the email information"
  opencount (Integer) Description : "Count of how many times email was opened"
  ownerid (String) Description : "Sales Person/Presales Engineer who created this email"
  owninguser (String) Description : "Sales Person/Presales Engineer who created this email"
  parentactivityid (Foreign key to DynamicsShortlisted.dbo.email.activityid) Description : "Activity due to which this email was send, it can be a follow up email to a parent activity"
  regardingobjectid (Foreign key to multiple tables) Description : "Object ID due to which this email was sent. It can be an account, contact, lead or opportunity"
  sendermailboxid (String) Description : "Email sender's mail box ID"
  sendersaccount (Foreign key to DynamicsShortlisted.dbo.account.accountid) Description : "Email sender's account ID"
  subject (String) Description : "Subject of the email"

General Table name: DynamicsShortlisted.dbo.lead
Description: 
Columns: 
  address1_city (String) Description : "Lead's city"
  address1_telephone1 (String) Description : "Telephone number of the lead"
  budgetamount (Decimal) Description : "Lead's budget amount"
  companyname (String) Description : "Company name of the lead"
  cr93b_sdr (String) Description : ""
  createdby (String) Description : "Sales Person/Presales Engineer who created this lead"
  createdon (Date) Description : "Date time on which this lead was created in the system"
  emailaddress1 (String) Description : "Email address of the lead"
  firstname (String) Description : "First name of the lead"
  jobtitle (String) Description : "Job title of the lead"
  lastname (String) Description : "Last name of the lead"
  msdyncrm_industry (String) Description : "Lead's industry"
  new_businessregion (String) Description : "Business region of the lead"
  new_ipbusinessregion (String) Description : "Business region IP address of the lead"
  new_originalsource (String) Description : "Source of the lead"
  new_outreach (String) Description : "Whether the lead was Inbound, outbound, or refferral"
  ownerid (String) Description : "Sales Person/Presales Engineer who created this lead"
  owninguser (String) Description : "Sales Person/Presales Engineer who is assigned this lead"
  parentaccountid (Foreign key to DynamicsShortlisted.dbo.account.accountid) Description : "Account ID of the lead"
  parentcontactid (Foreign key to DynamicsShortlisted.dbo.contact.contactid) Description : "Contact person's ID of the lead"
  subject (String) Description : "The product about which the lead sent the query for"
  telephone1 (String) Description : "Telephone number of the lead"
  websiteurl (String) Description : "Website URL of the lead's organization"
  xt_adcampaign (String) Description : "Add campaign that led to the lead signing up"
  xt_adgroup (String) Description : "Add group under which the add campaign was placed"
  xt_comments (String) Description : "Comments on how this lead was captured"
  xt_countrytext (String) Description : "Country of the lead"
  xt_description (String) Description : "Description of the lead capture"
  xt_emailvalidationstatus (String) Description : "Checks if lead's email is a valid email address"
  xt_falloutdetail (String) Description : "Details about why the lead was rejected or lost"
  xt_falloutreasons (Integer) Description : "Reasons about why the lead was rejected or lost"
  xt_firstconversion (String) Description : "First conversion event, article, campaign or video for this lead"
  xt_firstconversiondate (Date) Description : "First conversion's date time for this lead"
  xt_firstpageseen (String) Description : "Campaign or website's first pages seen by this lead"
  xt_firstreferringsite (String) Description : "The first site through which the lead navigated to Astera's Website"
  xt_industry (Integer) Description : "Industry of the lead"
  xt_inferredareacode (String) Description : "Area code of the lead"
  xt_inferredcountry2 (String) Description : "Country of the lead"
  xt_inferredstateregion (String) Description : "State or region of the lead"
  xt_ipcountry (String) Description : "Country of the IP address of the lead"
  xt_lastreferringsite (String) Description : "The site which refferred the lead to navigate to Astera's website"
  xt_leadproduct (Integer) Description : "Product in which the lead is interested in"
  xt_leadsource (Integer) Description : "Source of the lead"
  xt_leadstatus (Integer) Description : "Lead's status"
  xt_needdetails (String) Description : "Details about the needs of the lead"
  xt_notes (String) Description : "Notes about the lead's organization"
  xt_originalsearchengine (String) Description : "Search engine where the lead was browsing"
  xt_originalsearchphrase (String) Description : "Search Phrase due to which Astera's website URL showed up"
  xt_originalsource (Integer) Description : "Original source of the lead"
  xt_originalsourcedrilldown1 (String) Description : "Drill down information of the original source of the lead first part"
  xt_originalsourcedrilldown2 (String) Description : "Drill down information of the original source of the lead second part"
  xt_phonecode (Decimal) Description : "Phone code of the lead"
  xt_primarycontact (String) Description : "Primary contact person name of the lead"
  xt_primarycontactemail (String) Description : "Primary contact person's email address of the lead"
  xt_productnameis (Integer) Description : "Product name for which the lead is interested in"
  xt_referingdomain (String) Description : "Domain of the website which reffered Astera"
  xt_referingsite (String) Description : "Website of the referrer"
  xt_status (Integer) Description : "Status of the lead"
  xt_timeline (Integer) Description : "Timeline qouted by the lead"
  xt_utm_campaign (String) Description : "Campaign information that led to this lead's signup"
  xt_utm_content (String) Description : "Content information that lead to this lead's signup"
  xt_utm_medium (String) Description : "Medium where the add or content was posted"
  xt_utm_source (String) Description : "Source of the lead"
  xt_utm_term (String) Description : "Termsincluded in the add campaign"
  leadid (Primary Key) Description : "Lead ID"

General Table name: DynamicsShortlisted.dbo.opportunity
Description: 
Columns: 
  actualclosedate (Date) Description : "Actual closing date of the opportunity"
  actualvalue (Decimal) Description : "Actual value of the order placed by the opportunity"
  budgetamount (Decimal) Description : "Budget amount of the opportunity"
  cr93b_additionalrenewallicenserevenue (Decimal) Description : "Additional revenue generated by renewing license"
  cr93b_additionalrenewallicenserevenue_base (Decimal) Description : "Additional revenue generated by renewing license"
  cr93b_opportunityageindays (Integer) Description : "Days since the opportunity was created"
  cr93b_productmanagerforthisopportunity (String) Description : "Product manager who is looking after this opportunity. It depends on which product the opportunity is interested in."
  cr93b_sdr (String) Description : ""
  createdby (String) Description : "Sales Person/Presales Engineer who created this Opportunity"
  createdon (Date) Description : "Date time when this opportunity was created in the system"
  customerneed (String) Description : "Need of the opportunity, or use case notes"
  description (String) Description : "Query, description or notes on the opportunity"
  emailaddress (String) Description : "Email address of the opportunity"
  estimatedclosedate (Date) Description : "Estimated closing date of the opportunity"
  msdyn_forecastcategory (Integer) Description : "Forecast category"
  name (String) Description : "Name of the opportunity"
  new_commentsbypresales (String) Description : "Presales engineer's comments on the opportunity"
  new_democallrecordinglinks (String) Description : "Demo call recording links"
  new_demoprovided (Boolean) Description : "True if the demo is provided"
  new_discoverynotes (String) Description : "Discovery notes for this opportunity"
  new_dropoutexplanation (String) Description : "Explaination about why the opportunity dropped out"
  new_dropoutreason (Integer) Description : "Reason about why the opportunity dropped out"
  new_firstpageseen (String) Description : "The first page displayed to the opportunity"
  new_forecastedlicenserevenue (Decimal) Description : "Forecasted revenue from opportunity"
  new_forecastedlicenserevenue_base (Decimal) Description : "Forecasted revenue from opportunity"
  new_forecastedservicesrevenue (Decimal) Description : "Forecasted revenue from professional services with this opportunity"
  new_ipcountry (String) Description : "Country of the IP address of the Opportunity"
  new_lastcallbeforedropout (Integer) Description : "Last call before drop out ID"
  new_opportunityindustry (Integer) Description : "Industry of the opportunity"
  new_opportunitysecondaryproduct (Integer) Description : "If more than one products are considered, second product is mentioned here"
  new_originalsource (String) Description : "Source of this opportunity"
  new_supportingpressalesengineer (String) Description : "Presales engineer who is supporting or backing up the main presales engineer"
  opportunityid (Primary Key) Description : "ID of the opportunity"
  ownerid (String) Description : "Sales Person/Presales Engineer who created this Opportunity"
  owninguser (String) Description : "Sales Person/Presales Engineer whom this Opportunity is assigned to"
  parentaccountid (Foreign key to DynamicsShortlisted.dbo.account.accountid) Description : "Account ID for this opportunity"
  parentcontactid (Foreign key to DynamicsShortlisted.dbo.contact.contactid) Description : "Contact person for this opportunity"
  purchaseprocess (Integer) Description : "Purchase process for this opportunity"
  timeline (Integer) Description : "Timeline identified by the opportunity"
  xt_accounttype (Integer) Description : "Account type of the opportunity"
  xt_adcampaign (String) Description : "Add campaign that led to this opportunity signing up"
  xt_adgroup (String) Description : "Add group under which the add was ran to get this opportunity"
  xt_keywords (String) Description : "keywords used in the add"
  xt_lead (Foreign key to DynamicsShortlisted.dbo.lead.leadid) Description : "Lead ID of this opportunity"
  xt_leadsource (Integer) Description : "Lead source for this opportunity"
  xt_leadstatus (Integer) Description : "Lead status of this opportunity"
  xt_notes (String) Description : "Notes for this opportunity"
  xt_poccompleted (Boolean) Description : "Is Proof of concept (POC) complete for this opportunity"
  xt_pocprovidedby (String) Description : "Presales engineer who provided the POC"
  xt_productnameis (String) Description : "Product name in which the opportunity is interested in"

General Table name: DynamicsShortlisted.dbo.phonecall
Description: 
Columns: 
  activityid (Primary Key) Description : "Phone call ID"
  actualend (Date) Description : "Actual end date time of the phone call"
  actualstart (Date) Description : "Actual start date time of the phone call"
  createdby (String) Description : "The Sales person who made the phone call"
  createdon (Date) Description : "Date time when this phone call was made"
  description (String) Description : "Description of the phone call"
  ownerid (String) Description : "The Sales person who made the phone call"
  owninguser (String) Description : "The Sales person who made the phone call"
  phonenumber (String) Description : "The dialed phone number"
  regardingobjectid (Foreign key to multiple tables) Description : "The account, contact, lead or opportunity for which the phone call was made"
  scheduleddurationminutes (Integer) Description : "Scheduled time in minutes"
  scheduledend (Date) Description : "Scheduled end date time of the phone call"
  scheduledstart (Date) Description : "Scheduled start date time of the phone call"
  serviceid (String) Description : "Service ID of the phone call"
  subject (String) Description : "Subject or agenda of the phone call, can be description about the count of phone call too such as second phone call etc."

General Table name: DynamicsShortlisted.dbo.task
Description: 
Columns: 
  activityid (Primary Key) Description : "Task ID"
  actualend (Date) Description : "Task actual end date time"
  actualstart (Date) Description : "Task actual start date time"
  createdby (String) Description : "The Sales person or Presales engineer who created this task"
  createdon (Date) Description : "The date time when this task was created"
  description (String) Description : "Description of the task"
  ownerid (String) Description : "The person who this task is assigned to"
  owninguser (String) Description : "The person who this task is assigned to"
  regardingobjectid (Foreign key to multiple tables) Description : "The account, contact, lead or opportunity for which this task is created for"
  scheduledend (Date) Description : "Scheduled end date time of the task"
  scheduledstart (Date) Description : "Scheduled start date time of the task"
  subject (String) Description : "Subject of the task"

General Table name: DynamicsShortlisted.dbo.source
Description: Source table representing logical name details.
Columns:
  LogicalName (String) Description : "The logical name for source is: xt_leadsource"
  Value (Integer) Description : "The integer value representing the source"
  Label (String) Description : "The label describing the source value"

General Table name: DynamicsShortlisted.dbo.owner  
Description: Owner table representing details of system users and their roles.  
Columns:  
  fullname (String) Description: "The full name of the owner."  
  systemuserid (Integer) Description: "The system user ID associated with the owner."  
  ownerid (Integer) Description: "The unique identifier for the owner."  


General Table name: DynamicsShortlisted.dbo.dropout_reason
Description: Dropout Reason table representing logical name details.
Columns:
  DropoutReasonID (Integer) Description : "The identifier for the dropout reason"
  DropoutReason (String) Description : "The name or description of the dropout reason"


"""
    
    st.success("Successfully loaded database schema")
    return schema

def clean_sql_query(query: str) -> str:
    """Clean and format the SQL query"""
    # Remove any 'sql' prefix or suffix
    query = query.replace('```sql', '').replace('```', '').strip()
    
    # Remove any leading/trailing whitespace or semicolons
    query = query.strip().strip(';')
    
    # Add semicolon at the end
    query = f"{query};"
    
    return query

def generate_sql_query(user_query, db_schema):
    """Generate SQL query from natural language using LangChain"""
    prompt_template = """
    You are an expert SQL query generator. Given the following database schema and user question,
    generate a SQL query that will answer the user's question. The database is a CRM system tracking sales activities.
    
    Important Notes:
    1. Use proper table aliases for readability
    2. Consider relationships between tables when joining
    3. Use appropriate WHERE clauses to filter data
    4. Format dates using SQL Server date functions when needed
    5. Use appropriate aggregation functions when needed
    6. DO NOT include markdown code blocks or 'sql' prefix in your response
    7. Return ONLY the SQL query, nothing else
    8. IMPORTANT JOIN RULE: When querying dropout reasons, always join:
       opportunity.new_dropoutreason = dropout_reason.DropoutReasonID
       This will give you the actual dropout reason description from the dropout_reason table by using the dropout reason from the dropout reason table
    9. Whenever u include the sales rep id in a query also incluide that sales rep name. you can get that from fullname column in owner table
       
    Database Schema:
    {schema}
    
    User Question: {question}
    
    Generate only the SQL query without any explanation or markdown. The query should be valid SQL Server syntax.
    """
    
    prompt = PromptTemplate(
        template=prompt_template,
        input_variables=["schema", "question"]
    )
    
    formatted_prompt = prompt.format(schema=db_schema, question=user_query)
    response = llm.invoke([HumanMessage(content=formatted_prompt)])
    
    # Clean and format the query
    cleaned_query = clean_sql_query(response.content)
    
    return cleaned_query

def execute_sql_query(query):
    """Execute SQL query and return results as pandas DataFrame"""
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        "TrustServerCertificate=yes;"
    )
    
    try:
        st.info("Connecting to database...")
        conn = pyodbc.connect(conn_str)
        st.info("Executing query...")
        st.code(query, language="sql")  # Display the actual query being executed
        return pd.read_sql(query, conn)
    except pyodbc.Error as e:
        st.error(f"Database error: {str(e)}")
        return None
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def analyze_results(query_result, user_query):
    """Analyze query results and generate natural language response"""
    if query_result is None or query_result.empty:
        return "No results found for your query."
    
    prompt_template = """
    Given the following data results and the original user question, provide a clear and concise answer.
    
    User Question: {question}
    
    Data Results:
    {results}
    
    Please provide a natural language response that answers the user's question based on the data.
    """
    
    prompt = PromptTemplate(
        template=prompt_template,
        input_variables=["question", "results"]
    )
    
    formatted_prompt = prompt.format(
        question=user_query,
        results=query_result.to_string()
    )
    
    response = llm.invoke([HumanMessage(content=formatted_prompt)])
    return response.content.strip()

def create_streamlit_app():
    st.title("SQL Query Generator")
    
    # Get database schema
    db_schema = get_database_schema()
    if db_schema is None:
        st.error("Failed to connect to database. Please check your connection settings.")
        return
    
    # Display database schema in expandable sections
    with st.expander("View Complete Database Schema"):
        st.code(db_schema)
        
        # Add a download button for the schema
        st.download_button(
            label="Download Schema as Text",
            data=db_schema,
            file_name="database_schema.txt",
            mime="text/plain"
        )
    
    # User input
    user_query = st.text_area("Enter your question:", 
                             placeholder="Example: What are the total sales for each product category?")
    
    if st.button("Generate Answer"):
        if user_query:
            try:
                # Generate SQL query
                with st.spinner("Generating SQL query..."):
                    sql_query = generate_sql_query(user_query, db_schema)
                    st.subheader("Generated SQL Query:")
                    st.code(sql_query, language="sql")
                
                # Execute query
                with st.spinner("Executing query..."):
                    results = execute_sql_query(sql_query)
                    if results is not None:
                        st.subheader("Query Results:")
                        st.dataframe(results)
                        
                        # Add download button for results
                        csv = results.to_csv(index=False)
                        st.download_button(
                            label="Download Results as CSV",
                            data=csv,
                            file_name="query_results.csv",
                            mime="text/csv"
                        )
                        
                        # Analyze results
                        with st.spinner("Analyzing results..."):
                            analysis = analyze_results(results, user_query)
                            st.subheader("Answer:")
                            st.write(analysis)
                    
            except Exception as e:
                st.error(f"Error: {str(e)}")
        else:
            st.warning("Please enter a question.")

if __name__ == "__main__":
    create_streamlit_app() 