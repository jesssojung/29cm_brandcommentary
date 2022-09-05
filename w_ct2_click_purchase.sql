 SELECT  click.w
	   , click.ct1_name
       , click.ct2_name
       , c_user_cnt_ct
       , p_user_cnt_ct
 FROM
            (SELECT 
					 CASE WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-06-03' AND '2021-06-09' THEN '2021-06-03'
					   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-06-10' AND '2021-06-16' THEN '2021-06-10'
					   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-06-17' AND '2021-06-23' THEN '2021-06-17'
					   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-06-24' AND '2021-06-30' THEN '2021-06-24'
					   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-01' AND '2021-07-07' THEN '2021-07-01'
					   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-08' AND '2021-07-14' THEN '2021-07-08'
					   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-15' AND '2021-07-21' THEN '2021-07-15'
					   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-22' AND '2021-07-28' THEN '2021-07-22'
					   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-29' AND '2021-08-04' THEN '2021-07-29' END w
					 , category1_name ct1_name
					 , category2_name ct2_name
                     , COUNT(DISTINCT user_no) c_user_cnt_ct
			 FROM event e
				  INNER JOIN product p
                  ON e.item_no = p.item_no
             WHERE event_name = 'click_item'
             GROUP BY w, ct1_name, ct2_name) click
		LEFT JOIN 
			(SELECT 
					 CASE WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-06-03' AND '2021-06-09' THEN '2021-06-03'
					   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-06-10' AND '2021-06-16' THEN '2021-06-10'
					   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-06-17' AND '2021-06-23' THEN '2021-06-17'
					   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-06-24' AND '2021-06-30' THEN '2021-06-24'
					   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-01' AND '2021-07-07' THEN '2021-07-01'
					   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-08' AND '2021-07-14' THEN '2021-07-08'
					   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-15' AND '2021-07-21' THEN '2021-07-15'
					   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-22' AND '2021-07-28' THEN '2021-07-22'
					   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-29' AND '2021-08-04' THEN '2021-07-29' END w
					 , category1_name ct1_name
					 , category2_name ct2_name
                     , COUNT(DISTINCT user_no) p_user_cnt_ct
			 FROM event e
				  INNER JOIN product p
                  ON e.item_no = p.item_no
             WHERE event_name = 'purchase_success'
             GROUP BY w, ct1_name, ct2_name) purchase
			ON click.w = purchase.w
                AND click.ct1_name = purchase.ct1_name
                AND click.ct2_name = purchase.ct2_name

	