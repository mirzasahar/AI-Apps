import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
import json
import re
import pyodbc
from typing import Optional, Any, Tuple
import pandas as pd
from sqlalchemy import create_engine
import urllib

# Load environment variables
load_dotenv()

# Initialize API key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Initialize the language model
llm = ChatOpenAI(
    api_key=OPENAI_API_KEY,
    model="gpt-4",
    temperature=0.0
)

# Database schema
DB_SCHEMA = """
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

# General Table name: DynamicsShortlisted.dbo.annotation
# Description: 
# Columns: 
#   annotationid (Primary Key) Description : "ID of the annotation (note) object"
#   createdby (String) Description : "Sales Person/Presales engineer's ID who created this note"
#   createdbyname (String) Description : "Name of the Sales person/Presales engineer who created this note"
#   createdon (Date) Description : "Date on which this note was created"
#   documentbody (String) Description : "If the note has a document, this field contains the body of the document"
#   filename (String) Description : "Name of the file added in the note"
#   filesize (Integer) Description : "Size of the file added in the note"
#   isdocument (Boolean) Description : "true if the note is a document"
#   mimetype (String) Description : ""
#   modifiedon (Date) Description : "Modified date of the note"
#   notetext (String) Description : "Text in the note"
#   objectid (Foreign key to multiple tables) Description : "Object on which this note was created on. It can be an email, lead, phone call, contact, account, opportunity, appointment, or task"
#   overriddencreatedon (Date) Description : "If created on is overridden by the system, this field will contain its date of override"
#   ownerid (String) Description : "The Sales person/Presales Engineer's ID who created this note"
#   owneridname (String) Description : "Name of the Sales person/Presales engineer who created this note"
#   owninguser (String) Description : "Name of the current Sales person/Presales engineer who owns this note"
#   subject (String) Description : "Subject of the note"

# General Table name: DynamicsShortlisted.dbo.appointment
# Description: 
# Columns: 
#   activityid (Primary Key) Description : "Appointment ID"
#   actualend (Date) Description : "Actual end datetime of the appointment"
#   createdby (String) Description : "Sales person/Presales engineer who created the appointment"
#   createdon (Date) Description : "Date time on which the appointment was created"
#   description (String) Description : "Description of the appointment"
#   globalobjectid (String) Description : "Global object ID of the appointment"
#   location (String) Description : "Location or platform of the appointment"
#   ownerid (String) Description : "Sales person/Presales engineer who is assigned the appointment"
#   owninguser (String) Description : "Sales person/Presales engineer who is assigned the appointment"
#   regardingobjectid (Foreign key to multiple tables) Description : "Account, lead or item for which the appointment is created"
#   scheduleddurationminutes (Integer) Description : "Scheduled duration in minutes"
#   scheduledend (Date) Description : "Scheduled end time of the appointment"
#   scheduledstart (Date) Description : "Scheduled start time of the appointment"
#   subject (String) Description : "Subject or objective of the appointment"
#   xt_addteamsmeetinglink (Boolean) Description : "Added teams meeting link or not"
#   xt_recordinglink (String) Description : "Recording link of the meeting"
#   xt_teamsmeetingurl (String) Description : "Microsoft teams meeting URL"

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

General Table name: DynamicsShortlisted.dbo.source
Description: Source table representing logical name details.
Columns:
  LogicalName (String) Description : "The logical name for source is: xt_leadsource"
  Value (Integer) Description : "The integer value representing the source"
  Label (String) Description : "The label describing the source value"

General Table name: DynamicsShortlisted.dbo.owner  
Description: Owner table representing details of system users and their roles.  
Columns:  
  fullname (String) Description: "The full name of the owner/sales rep."  
  ownerid (Integer) Description: "The unique identifier for the owner."  


General Table name: DynamicsShortlisted.dbo.dropout_reason
Description: Dropout Reason table representing logical name details.
Columns:
  DropoutReasonID (Integer) Description : "The identifier for the dropout reason"
  DropoutReason (String) Description : "The dropout reason"
"""

# Updated triage prompt template
TRIAGE_PROMPT = """You are a query classifier for a CRM database system. You have access to the following database schema:

{schema}

Analyze if the question can be answered using the available database tables and fields.
Categorize questions into three types:
1. DATA_QUESTION: Questions that require querying the database, including questions that need data analysis after querying
   Examples:
   - "How many opportunities dropped out?"
   - "What are the common dropout reasons?"
   - "Analyze dropout patterns and suggest strategies"
   - "What's the performance of sales reps based on their conversion rates?"

2. GENERAL_QUESTION: Questions about CRM concepts that cannot be answered even partially from the database
   Examples:
   - "What is the best CRM strategy?"
   - "How should we train new sales reps?"
   - "What are industry best practices for reducing dropouts?"

3. OUT_OF_SCOPE: Questions unrelated to CRM or data analysis
   Examples:
   - "What's the weather like?"
   - "How do I make coffee?"

Important: If a question requires analyzing data from the database FIRST (even if it also needs interpretation after), classify it as DATA_QUESTION.

Question: {question}

Return ONLY a valid JSON object in this exact format:
{{"queryType": "DATA_QUESTION" | "GENERAL_QUESTION" | "OUT_OF_SCOPE"}}"""

# Update the schema analysis prompt to be more explicit
SCHEMA_ANALYSIS_PROMPT = """You are a database expert. Analyze if the question can be answered using the available tables and fields.
Available Schema:
{schema}

Question: {question}

You must return a JSON response with EXACTLY this structure:
{{
    "isAnswerable": true or false,
    "outOfScopeReason": "Reason if not answerable, null if answerable",
    "relevantTables": [
        {{
            "tableName": "Full table name including schema",
            "fields": ["field1", "field2"],
            "reason": "Why this table is needed"
        }}
    ],
    "relationships": ["table1.field1 → table2.field2"],
    "conditions": ["Any WHERE conditions needed"]
}}

Example response for "Show dropped out opportunities":
{{
    "isAnswerable": true,
    "outOfScopeReason": null,
    "relevantTables": [
        {{
            "tableName": "DynamicsShortlisted.dbo.opportunity",
            "fields": ["opportunityid", "new_dropoutreason", "customerneed"],
            "reason": "Contains dropout information and use cases"
        }}
    ],
    "relationships": [],
    "conditions": ["new_dropoutreason IS NOT NULL"]
}}"""

# Use the provided SQL generation prompt
SQL_GENERATION_PROMPT = """
You are an expert SQL query generator for a CRM database. Given a user question, create a syntactically correct SQL Server query.

IMPORTANT RULES:
1. NEVER use INSERT, UPDATE, DELETE, or DROP statements
2. Unless specified, limit results to top 100 records
3. Only select relevant columns that answer the question
4. Always use proper table aliases (e.g., 'o' for opportunity, 'dr' for dropout_reason)
5. Order results by relevant columns to show most important data first

ID FIELD HANDLING:
1. For any ID fields, always remove curly braces using:
   REPLACE(REPLACE(field_name, '{{', ''), '}}', '') 
2. Common ID fields to clean:
   - ownerid
   - opportunityid
   - leadid
   - contactid
   - accountid

ANALYTICAL QUERIES:
1. For performance analysis:
   - Use COUNT(DISTINCT) for accurate counts
   - Use CAST AS FLOAT for ratios and percentages
   - Use NULLIF to prevent division by zero
   - Include relevant aggregations (SUM, AVG, etc.)

2. For ratios and rates:
   - Calculate as CAST(numerator AS FLOAT) / NULLIF(denominator, 0)
   - Include both raw counts and calculated ratios

Previous Query (if any): {previous_query}
Error Message (if any): {error_message}

Database Schema:
{schema}

User Question: {question}

Generate only the SQL query without any explanation or markdown. The query should be valid SQL Server syntax.
"""

def clean_json_response(response: str) -> str:
    """Clean and validate JSON response from LLM"""
    # Remove code block markers
    cleaned = re.sub(r'```json\s*|\s*```', '', response)
    
    # Remove any invalid control characters
    cleaned = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', cleaned)
    
    # Ensure it's valid JSON
    try:
        json.loads(cleaned)
        return cleaned
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {cleaned}")  # Debug print
        raise e

def triage_query(question: str) -> str:
    """Determine the type of query"""
    prompt = TRIAGE_PROMPT.format(schema=DB_SCHEMA, question=question)
    response = llm.invoke([HumanMessage(content=prompt)])
    cleaned_response = clean_json_response(response.content)
    result = json.loads(cleaned_response.strip())
    return result["queryType"]

def generate_general_response(question: str) -> str:
    """Generate a response for general CRM questions"""
    prompt = """You are a CRM expert. Generate a helpful response to this general CRM question. 
    Focus on best practices and industry knowledge. Keep the response concise and practical.
    
    Question: {question}
    """
    
    response = llm.invoke([HumanMessage(content=prompt.format(question=question))])
    return response.content

def handle_out_of_scope(question: str) -> str:
    """Handle out of scope questions with a polite response"""
    return ("I apologize, but this question is outside the scope of our CRM system. "
            "I can help you with questions about sales, opportunities, leads, and other CRM-related topics.")

def analyze_schema(question: str, schema: str) -> tuple[bool, str, dict]:
    """Analyze which tables and fields are needed to answer the question"""
    prompt = SCHEMA_ANALYSIS_PROMPT.format(schema=schema, question=question)
    response = llm.invoke([HumanMessage(content=prompt)])
    
    # Debug print
    print("\nSchema Analysis Response:")
    print(response.content)
    
    cleaned_response = clean_json_response(response.content)
    analysis = json.loads(cleaned_response.strip())
    
    # Verify required fields exist
    if "isAnswerable" not in analysis:
        raise ValueError("Schema analysis response missing 'isAnswerable' field")
    
    return (
        analysis["isAnswerable"],
        analysis.get("outOfScopeReason"),
        analysis
    )

def validate_and_clean_sql(query: str) -> str:
    """Validate and clean SQL query"""
    # Replace problematic characters
    query = query.replace("'", '"')  # Replace single quotes with double quotes
    query = query.replace('\n', ' ')  # Remove newlines
    query = ' '.join(query.split())   # Remove extra whitespace
    
    # Add basic validation
    if not query.strip().upper().startswith('SELECT'):
        raise ValueError("Query must start with SELECT")
    
    return query

def generate_sql_query(question: str, schema_analysis: dict) -> tuple[str, str]:
    """Generate SQL query based on schema analysis"""
    prompt = f"""
    You are an expert SQL query generator for a CRM database. Given a user question, create a syntactically correct SQL Server query.
    
    IMPORTANT: For questions about "most" or "top", show ALL records ordered by the metric unless specifically asked for a limit.
    
    Example:
    For "Which sales rep generated the most leads?", use:
    SELECT 
        o.fullname as sales_rep_name,
        COUNT(l.leadid) AS total_leads,
        COUNT(DISTINCT l.leadid) AS unique_leads
    FROM DynamicsShortlisted.dbo.lead l 
    JOIN DynamicsShortlisted.dbo.owner o 
        ON REPLACE(REPLACE(l.createdby, '{{', ''), '}}', '') = REPLACE(REPLACE(o.ownerid, '{{', ''), '}}', '')
    GROUP BY o.fullname
    ORDER BY total_leads DESC;

    Schema Analysis: {json.dumps(schema_analysis, indent=2)}
    Question: {question}

    Return ONLY a valid JSON object with this exact structure:
    {{
        "query": "YOUR SQL QUERY HERE",
        "explanation": "Brief explanation of the query"
    }}
    """
    
    response = llm.invoke([HumanMessage(content=prompt)])
    cleaned_response = clean_json_response(response.content)
    result = json.loads(cleaned_response)
    
    return result["query"], result.get("explanation", "")

def generate_alternative_query(original_query: str, error_message: str, user_query: str, schema: str) -> str:
    """Generate alternative SQL query based on error message"""
    prompt = SQL_GENERATION_PROMPT.format(
        schema=schema,
        question=user_query,
        previous_query=original_query,
        error_message=error_message
    )
    response = llm.invoke([HumanMessage(content=prompt)])
    return response.content.strip().strip('`').strip()

def execute_with_retry(query: str, user_query: str, schema: str, max_attempts: int = 3) -> Tuple[bool, Optional[pd.DataFrame], str]:
    """Execute query with intelligent retry logic"""
    db = DatabaseConnection()
    attempt = 0
    current_query = query
    last_error = ""
    
    while attempt < max_attempts:
        try:
            print(f"\nAttempt {attempt + 1} - Executing query:")
            print(current_query)
            
            df = db.execute_query(current_query)
            
            # Verify results make sense
            if df is not None and not df.empty:
                return True, df, "Success"
            else:
                last_error = "Query returned no results"
                
        except Exception as e:
            last_error = str(e)
            print(f"Error: {last_error}")
            
        # Generate alternative query based on error
        attempt += 1
        if attempt < max_attempts:
            print(f"\nGenerating alternative query based on error...")
            current_query = generate_alternative_query(
                original_query=current_query,
                error_message=last_error,
                user_query=user_query,
                schema=schema
            )
            continue
    
    return False, None, f"Failed after {max_attempts} attempts. Last error: {last_error}"

def generate_data_response(df: pd.DataFrame, user_query: str) -> str:
    """Generate a direct answer to the user's question using query results"""
    # Limit the data to top 20 rows to avoid context length issues
    sample_data = df.head(20).to_dict('records')
    
    prompt = f"""
    You are the AskAstera assistant, a database expert that explains query results in clear, natural language.
    Provide a concise answer that directly addresses the user's question based on the query results.

    User Question: {user_query}
    Total Records Found: {len(df)}
    Sample Data: {sample_data}
    
    Respond in JSON format matching this schema:
    {{
        "user_query": "{user_query}",
        "answer": "string"
    }}
    """
    
    response = llm.invoke([HumanMessage(content=prompt)])
    cleaned_response = clean_json_response(response.content)
    result = json.loads(cleaned_response)
    return result["answer"]

def validate_answer(question: str, answer: str) -> tuple[bool, str]:
    """Validate if the answer is reasonable for the given question"""
    prompt = f"""
    You are the final step of a data analysis pipeline - a final quality check if you will.
    Determine if the provided answer is reasonable for the given question.
    Most of the time, the answer will be adequate - even if the contents are fictional or made up.
    Do not reject answers that are not perfect, as long as they are reasonable.

    Question: {question}
    Answer: {answer}

    Respond in JSON format matching this schema:
    {{
        "isValid": true/false,
        "reason": "string explaining why the answer is valid or invalid",
        "suggestedFix": "string with suggestion if invalid, null if valid"
    }}
    """
    
    response = llm.invoke([HumanMessage(content=prompt)])
    result = json.loads(clean_json_response(response.content))
    
    return result["isValid"], result.get("reason", ""), result.get("suggestedFix")

def process_query(user_query: str) -> tuple[str, Optional[pd.DataFrame], str]:
    """Process a user query through the complete pipeline"""
    try:
        # Step 1: Initial Triage
        query_type = triage_query(user_query)
        print(f"\nTriage Result: {query_type}")
        
        # Handle non-data questions
        if query_type == "GENERAL_QUESTION":
            return generate_general_response(user_query), None, "GENERAL_QUESTION"
        
        if query_type == "OUT_OF_SCOPE":
            return handle_out_of_scope(user_query), None, "OUT_OF_SCOPE"
        
        # Step 2: Schema Analysis for Data Questions
        try:
            is_answerable, out_of_scope_reason, schema_analysis = analyze_schema(user_query, DB_SCHEMA)
            
            if not is_answerable:
                return f"This question cannot be answered using the available data: {out_of_scope_reason}", None, "OUT_OF_SCOPE"
            
            # Step 3: Generate initial SQL Query
            sql_query, _ = generate_sql_query(user_query, schema_analysis)
            print("\nInitial SQL Query:")
            print(sql_query)
            
            # Step 4: Execute Query with intelligent retry
            success, results, message = execute_with_retry(
                query=sql_query,
                user_query=user_query,
                schema=DB_SCHEMA,
                max_attempts=3
            )
            
            if not success:
                return f"Failed to execute query: {message}", None, "ERROR"
            
            if results is None or results.empty:
                return "No data found for your query.", None, "DATA_QUESTION"
                
            # Generate initial response
            response = generate_data_response(results, user_query)
            
            # Validate the response
            print("\n🔍 Generated Initial Response:")
            print(response)
            
            print("\n✨ Validating Response...")
            is_valid, reason, suggested_fix = validate_answer(user_query, response)
            print(f"Valid: {is_valid}")
            print(f"Reason: {reason}")
            if suggested_fix:
                print(f"Suggested Fix: {suggested_fix}")
            
            if not is_valid:
                print("\n🔄 Generating improved response...")
                if suggested_fix:
                    response = generate_data_response(results, user_query + " " + suggested_fix)
                    print(f"New Response: {response}")
                
            return response, results, "DATA_QUESTION"
            
        except Exception as e:
            print(f"\nError in query processing: {str(e)}")
            return f"Error processing query: {str(e)}", None, "ERROR"
        
    except Exception as e:
        return f"Error processing query: {str(e)}", None, "ERROR"

class DatabaseConnection:
    def __init__(self):
        self.server = os.getenv("SQL_SERVER")
        self.database = os.getenv("SQL_DATABASE")
        self.username = os.getenv("SQL_USERNAME")
        self.password = os.getenv("SQL_PASSWORD")
        self.engine = None
        
    def connect(self):
        try:
            params = urllib.parse.quote_plus(
                f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                f'SERVER={self.server};'
                f'DATABASE={self.database};'
                f'UID={self.username};'
                f'PWD={self.password}'
            )
            
            connection_url = f'mssql+pyodbc:///?odbc_connect={params}'
            self.engine = create_engine(connection_url)
            return True
        except Exception as e:
            print(f"Database connection error: {str(e)}")
            return False
            
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        try:
            if not self.engine:
                if not self.connect():
                    raise Exception("Failed to establish database connection")
            return pd.read_sql_query(query, self.engine)
        except Exception as e:
            print(f"Query execution error: {str(e)}")
            raise

# Test examples including validation failures
if __name__ == "__main__":
    test_questions = [
        "For the opportunities that dropped out because the product 'Doesn't Accomplish the Task', identify from the use case which product was missing.",
        "Looking at the dropout reasons and explanations, suggest actionable strategies to reduce our dropout rates.",
        "Analyze the opportunities that dropped out based on sales reps’ performance. Include their opportunities-to-leads ratio.",
        "How many leads were created last month?",  # if response talks about opportunities instead
        "What are the top reasons for opportunity dropouts and their counts?"  # if response only lists reasons without counts
    ]

    for i, question in enumerate(test_questions, 1):
        print(f"\n{'='*50}")
        print(f"Test {i}:")
        print(f"Question: {question}")
        try:
            response, results, query_type = process_query(question)
            print(f"\nFinal Response:")
            print(response)
        except Exception as e:
            print(f"Error: {str(e)}")