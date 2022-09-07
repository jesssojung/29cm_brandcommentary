SELECT click.w -- 주
 		 , click.ct1_name -- 카테고리 대분류
		 , click.ct2_name -- 카테고리 중분류
		 , click.brd_name -- 브랜드
         , brd_gmv -- 주차별 브랜드 매출
         , ct2_cnt_pct -- 카테고리 중분류 브랜드 개수, 태블로 조회 시 필터에 중분류 및 주차 필수
         , ct2_gmv_pct -- 카테고리 중분류 브랜드 매출, 태블로 조회 시 내림차순 필터에 중분류 및 주차 필수 
         , brd_p_user_cnt -- 주차별 브랜드 구매유저수
         , brd_c_user_cnt -- 주차별 브랜드 클릭 유저수
         , brd_p_user_cnt / brd_c_user_cnt cv -- 주차별 브랜드 구매 전환율 (대체 지표)
         , SUM(brd_gmv) OVER(PARTITION BY w, ct2_name, brd_name) / SUM(gmv.brd_gmv) OVER(PARTITION BY gmv.w, ct2_name) brd_pct -- 주차별 중분류 내 브랜드 매출 비중
FROM
             ( -- 주단위 click event 테이블
             SELECT CASE WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-06-03' AND '2021-06-09' THEN '2021-06-03'
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
                     , brand_name brd_name
                     , COUNT(DISTINCT user_no) brd_c_user_cnt
			 FROM event e
				  INNER JOIN product p
                  ON e.item_no = p.item_no
             WHERE event_name = 'click_item'
             GROUP BY w, ct1_name, ct2_name, brand_name) click
		LEFT JOIN
		( -- 주차별 purchase event 테이블 - 파레토 법칙 확인, 주차별 브랜드별 구매 유저수 & gmv
		SELECT purchase.w
				 , ct1_name
				 , ct2_name
				 , brd_name
				 , brd_gmv
				 , SUM(brd_cnt) OVER(PARTITION BY w, ct1_name, ct2_name ORDER BY idx) / SUM(brd_cnt) OVER(PARTITION BY w, ct1_name, ct2_name) AS ct2_cnt_pct
				 , SUM(brd_gmv) OVER(PARTITION BY w, ct1_name, ct2_name ORDER BY idx) / SUM(brd_gmv) OVER(PARTITION BY w, ct1_name, ct2_name) AS ct2_gmv_pct
				 , brd_p_user_cnt
		FROM
				(
				SELECT ROW_NUMBER() OVER(ORDER BY w, ct1_name, ct2_name, brd_gmv DESC) idx
					 , w
					 , ct1_name
					 , ct2_name
					 , brd_name
					 , brd_cnt
					 , brd_gmv
					 , brd_p_user_cnt
				FROM
					( -- 주단위 구매액,  구매 유저 수
					SELECT CASE WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-06-03' AND '2021-06-09' THEN '2021-06-03'
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
						 , brand_name brd_name
						 , 1 as brd_cnt
						 , SUM(price) brd_gmv
						 , COUNT(DISTINCT user_no) brd_p_user_cnt
					FROM event e INNER JOIN product p
						 ON e.item_no = p.item_no
					WHERE event_name = 'purchase_success'
					GROUP BY w, category1_name, category2_name, brand_name) sub
			) purchase
		ORDER BY w, ct1_name, ct2_name, brd_gmv DESC
    ) gmv
ON click.w = gmv.w
	AND click.ct1_name = gmv.ct1_name
	AND click.ct2_name = gmv.ct2_name
	AND click.brd_name = gmv.brd_name
ORDER BY click.w, click.ct1_name, click.ct2_name, gmv.brd_gmv DESC
