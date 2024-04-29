

  create view `xapi`.`dim_user_pii` 
  
    
    
  as (
    select external_user_id, username, name, email from `xapi`.`user_pii` user_pii
  )
      
      
                    -- end_of_sql
                    
                    