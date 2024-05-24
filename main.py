import streamlit as st
import pandas as pd
import snowflake.connector

# Paramètres de connexion Snowflake
connection_parameters = {
    "account": "kf52388.eu-west-1",  
    "user": "LUAP",                  
    "password": "Luapk989",          
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "linkedin",          
    "schema": "public"
}

try:
    # Connexion à Snowflake
    conn = snowflake.connector.connect(
        user=connection_parameters['user'],
        password=connection_parameters['password'],
        account=connection_parameters['account'],
        warehouse=connection_parameters['warehouse'],
        database=connection_parameters['database'],
        schema=connection_parameters['schema'],
        role=connection_parameters['role']
    )
    
   
    # Requête pour obtenir le titre de poste le mieux rémunéré
    highest_salary_query = """
    SELECT title, max_salary, currency
    FROM Jobs_posting
    ORDER BY max_salary DESC
    LIMIT 1;
    """
    
    # Requête pour obtenir le top 10 des titres de postes les plus postés
    top_titles_query = """
    SELECT title, COUNT(*) AS post_count
    FROM Jobs_posting
    GROUP BY title
    ORDER BY post_count DESC
    LIMIT 10;
    """
    
    # Requête pour la répartition des offres d’emploi par taille d’entreprise
    job_by_company_size_query = """
    SELECT c.company_size, COUNT(*) AS job_count
    FROM Jobs_posting jp
    JOIN Companies c ON jp.company_id = c.company_id
    GROUP BY c.company_size
    ORDER BY job_count DESC;
    """
    
    # Requête pour la répartition des offres d’emploi par type d’industrie
    job_by_industry_query = """
    SELECT i.industry_name, COUNT(*) AS job_count
    FROM Jobs_posting jp
    JOIN Job_Industries ji ON jp.job_id = ji.job_id
    JOIN Industries i ON ji.industry_id = i.industry_id
    GROUP BY i.industry_name
    ORDER BY job_count DESC;
    """
    
    # Requête pour la répartition des offres d’emploi par type d’emploi (full-time, internship, part-time)
    job_by_type_query = """
    SELECT formatted_work_type, COUNT(*) AS job_count
    FROM Jobs_posting
    GROUP BY formatted_work_type
    ORDER BY job_count DESC;
    """
    
    # Exécuter les requêtes et obtenir les résultats
    highest_salary_df = pd.read_sql(highest_salary_query, conn)
    top_titles_df = pd.read_sql(top_titles_query, conn)
    job_by_company_size_df = pd.read_sql(job_by_company_size_query, conn)
    job_by_industry_df = pd.read_sql(job_by_industry_query, conn)
    job_by_type_df = pd.read_sql(job_by_type_query, conn)
    
    # Fermer la connexion
    conn.close()
    
    # Titre de l'application
    st.title("Analyse des postes LinkedIn")

    # Afficher le titre de poste le mieux rémunéré
    st.header("Titre de poste le mieux rémunéré")
    st.dataframe(highest_salary_df)
    
    # Afficher le top 10 des titres de postes les plus postés
    st.header("Top 10 des titres de postes les plus postés")
    st.dataframe(top_titles_df)
    
    # Afficher la répartition des offres d’emploi par taille d’entreprise
    st.header("Répartition des offres d’emploi par taille d’entreprise")
    st.dataframe(job_by_company_size_df)
    
    # Afficher la répartition des offres d’emploi par type d’industrie
    st.header("Répartition des offres d’emploi par type d’industrie")
    st.dataframe(job_by_industry_df)
    
    # Afficher la répartition des offres d’emploi par type d’emploi (full-time, internship, part-time)
    st.header("Répartition des offres d’emploi par type d’emploi")
    st.dataframe(job_by_type_df)
    
except Exception as e:
    st.error(f"Erreur lors de la connexion à Snowflake : {e}")