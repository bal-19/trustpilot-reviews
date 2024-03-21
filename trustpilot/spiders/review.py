from datetime import datetime
from pathlib import Path
from uuid import uuid4
import logging
import scrapy
import s3fs
import json
import re
import os



class ReviewSpider(scrapy.Spider):
    name = "review"
    total_success = 0
    total_failed = 0

    def start_requests(self):
        # =====================================================================
        url = "https://www.trustpilot.com/review/indriver.com?languages=all"
        # =====================================================================
        yield scrapy.Request(url=url, callback=self.parse)

    # =====================================================================
    def upload_to_s3(self, rpath, lpath):
        client_kwargs = {
            'key': 'YOUR_S3_KEY',
            'secret': 'YOUR_S3_SECRET',
            'endpoint_url': 'YOUR_ENDPOINT',
            'anon': False
        }

        s3 = s3fs.core.S3FileSystem(**client_kwargs)

        # Upload file
        s3.upload(rpath=rpath, lpath=lpath)
    # =====================================================================
    
    def log_error(self, crawling_time, id_project, project, sub_project, source_name, sub_source_name, id_sub_source, id_data, process_name, status, type_error, message, assign, path):
        log_error = {
            "crawlling_time": crawling_time,
            "id_project": id_project,
            "project": project,
            "sub_project": sub_project,
            "source_name": source_name,
            "sub_source_name": sub_source_name,
            "id_sub_source": id_sub_source,
            "id_data": id_data,
            "process_name": process_name,
            "status": status,
            "type_error": type_error,
            "message": message,
            "assign": assign
        }
        
        try:
            with open(path, 'r') as file:
                existing_data = json.load(file)
        except FileNotFoundError:
            existing_data = []

        existing_data.append(log_error)

        with open(path, 'w') as file:
            json.dump(existing_data, file)
            
            
    def log(self, crawling_time, id_project, project, sub_project, source_name, sub_source, id_sub_source, total, total_success, total_failed, status, assign, path):
        log = {
            'crawling_time': crawling_time,
            'id_project': id_project,
            'project': project,
            'sub_project': sub_project,
            'source_name': source_name,
            'sub_source_name': sub_source,
            'id_sub_source': id_sub_source,
            'total_data': int(total),
            'total_success': total_success,
            'total_failed': total_failed,
            'status': status,
            'assign': assign,
        }
        
        try:
            with open(path, 'r') as file:
                existing_data = json.load(file)
        except FileNotFoundError:
            existing_data = []

        existing_data.append(log)

        with open(path, 'w') as file:
            json.dump(existing_data, file)
    

    def parse(self, response):
        # logging
        id_project = None
        project = 'data intelligence'
        sub_project = 'data review'
        assign = 'iqbal'
        
        url = response.url
        domain = url.split('/')[2]
        sub_source = url.split('/')[4].split('?')[0]
        id_sub_src = int(str(uuid4()).replace('-', ''), 16)
        category_reviews = 'service'
        crawling_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        crawling_time_epoch = int(datetime.now().timestamp() * 1000)
        # ===========================================================
        # YOUR S3 PATH
        path_data_raw = f's3://{domain}/{sub_source}/json'
        path_data_clean = f's3://{domain}/{sub_source}/json'
        # ===========================================================
        
        # copmany information
        company = response.css('#business-unit-title > h1 > span.typography_display-s__qOjh6.typography_appearance-default__AAY17.title_displayName__TtDDM::text').get()
        tag = [domain, category_reviews, company]
        total_reviews = response.css('#__next > div > div > main > div > div.styles_mainContent__nFxAv > section > div.paper_paper__1PY90.paper_outline__lwsUX.card_card__lQWDv.styles_reviewsOverview__mVIJQ > div.styles_header__yrrqf > p::text').get().split(' ')[0].replace(',','')
        overall = response.xpath('//*[@id="business-unit-title"]/span/span/text()[5]').get()
        total_rating = response.css('#business-unit-title > div > div > p::text').get()
        
        # scraping reviews
        for reviews in response.css('#__next > div > div > main > div > div.styles_mainContent__nFxAv > section > div.styles_cardWrapper__LcCPA').getall():
            try:
                review = scrapy.Selector(text=reviews)
                username = review.css('article > div > aside > div > a > span::text').get()
                avatar = review.css('article > div > aside > div > div > span > img[src^="http"]::attr(src)').get()
                if avatar is None:
                    avatar = ''
                location_reviews = review.css('article > div > aside > div > a > div > div > span::text').get()
                title_detail_reviews = review.css('article > div > section > div.styles_reviewContent__0Q2Tg > a > h2::text').get()
                reviews_rating = review.css('article > div > section > div.styles_reviewHeader__iU9Px::attr(data-service-review-rating)').get()
                total_likes_reviews = 0
                content_reviews = review.css('article > div > section > div.styles_reviewContent__0Q2Tg > p.typography_body-l__KUYFJ.typography_appearance-default__AAY17.typography_color-black__5LYEn::text').get()
                date_of_exp = review.xpath('//*[@data-service-review-date-of-experience-typography="true"]/text()[2]').get()
                date_of_exp = datetime.strptime(date_of_exp, "%B %d, %Y")
                date_of_exp = date_of_exp.strftime("%Y-%m-%d %H:%M:%S")
                date_of_exp_epoch = int(datetime.strptime(date_of_exp, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
                created_time = review.css('article > div > section > div.styles_reviewHeader__iU9Px > div.typography_body-m__xgxZ_.typography_appearance-subtle__8_H2l.styles_datesWrapper__RCEKH > time::attr(datetime)').get()
                if created_time is not None:
                    created_time = datetime.strptime(created_time, "%Y-%m-%dT%H:%M:%S.%fZ")
                    created_time = created_time.strftime("%Y-%m-%d %H:%M:%S")
                    created_time_epoch = int(datetime.strptime(created_time, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
                else:
                    created_time = date_of_exp
                    created_time_epoch = date_of_exp_epoch
                total_reply = 0
                reply_container = review.css('article > div > div.paper_paper__1PY90.paper_outline__lwsUX.paper_subtle__lwJpX.card_card__lQWDv.card_noPadding__D8PcU.styles_wrapper__ib2L5')
                reply = []
                if reply_container:
                    username_reply = reply_container.css('div.styles_content__Hl2Mi > div > p::text').get()
                    username_reply = username_reply.replace('Reply From ', '')
                    content_reply = reply_container.css('div.styles_content__Hl2Mi > p::text').get()
                    total_reply = 1
                    
                    reply.append({
                        'username_reply_reviews' : username_reply,
                        'content_reviews' : content_reply
                    })
                    
                file_name = f'{company.replace(" ", "_").lower()}_{created_time_epoch}.json'   
                
                company_information = {
                    'link' : url,
                    'domain' : domain,
                    'tag' : tag,
                    'crawling_time' : crawling_time,
                    'crawling_time_epoch' : crawling_time_epoch,
                    'path_data_raw' : f'{path_data_raw}/{file_name}',
                    'path_data_clean' : f'{path_data_clean}/{file_name}',
                    'reviews_name' : company,
                    'location_reviews' : None,
                    'category_reviews' : category_reviews,
                    'total_reviews' : int(total_reviews),
                    'overall' : overall,
                    'reviews_rating' : {
                        'total_rating' : float(total_rating),
                        'detail_total_rating' : [
                            {
                                'score_rating' : None,
                                'category_rating' : None
                            }
                        ]
                    }
                }

                review_info = {
                    'detail_reviews' : {
                        'username_reviews' : username,
                        'image_reviews' : avatar,
                        'created_time' : created_time,
                        'created_time_epoch' : created_time_epoch,
                        'email_reviews' : None,
                        'company_name' : company,
                        'location_reviews' : location_reviews,
                        'title_detail_reviews' : title_detail_reviews,
                        'reviews_rating' : float(reviews_rating),
                        'detail_reviews_rating' : [
                            {
                                "score_rating": None,
                                "category_rating": None
                            }
                        ],
                        'total_likes_reviews' : None,
                        'total_dislikes_reviews' : None,
                        'total_reply_reviews' : total_reply,
                        'content_reviews' : content_reviews,
                        'reply_content_reviews' : reply,
                        'date_of_experience' : date_of_exp,
                        'date_of_experience_epoch' : date_of_exp_epoch
                    }
                }
                
                data = {**company_information, **review_info}
                
                # ======================================================
                my_dir = 'F:/Work/Crawling Trust Pilot/data'
                # ======================================================
                if not os.path.exists(my_dir):
                    os.makedirs(my_dir)
                    
                with open(f'{my_dir}/{file_name}', 'w') as f:
                    json.dump(data, f) 
                
                # upload to s3
                self.upload_to_s3(f'{path_data_raw.replace('s3://', '')}/{file_name}', f'{my_dir}/{file_name}')
                    
                self.total_success += 1
                self.log_error(crawling_time, id_project, project, sub_project, domain, sub_source, id_sub_src, crawling_time_epoch, 'crawling', 'success', '', '', assign, 'F:/Work/Crawling Trust Pilot/log_error.json')
            
            except Exception as e:
                self.total_failed += 1
                self.log_error(crawling_time, id_project, project, sub_project, domain, sub_source, id_sub_src, crawling_time_epoch, 'crawling', 'error', type(e).__name__, str(e), assign, 'F:/Work/Crawling Trust Pilot/log_error.json')
                
        
        # pagination
        next_page = response.css('#__next > div > div > main > div > div.styles_mainContent__nFxAv > section > div.styles_pagination__6VmQv > nav > a.link_internal__7XN06.button_button__T34Lr.button_m__lq0nA.button_appearance-outline__vYcdF.button_squared__21GoE.link_button___108l.pagination-link_next__SDNU4.pagination-link_rel__VElFy::attr(aria-disabled)').get()
        if next_page is None:
            next_page_link = response.css('#__next > div > div > main > div > div.styles_mainContent__nFxAv > section > div.styles_pagination__6VmQv > nav > a.link_internal__7XN06.button_button__T34Lr.button_m__lq0nA.button_appearance-outline__vYcdF.button_squared__21GoE.link_button___108l.pagination-link_next__SDNU4.pagination-link_rel__VElFy::attr(href)').get()
            yield scrapy.Request(url=response.urljoin(next_page_link), callback=self.parse)
        elif next_page == 'true':
            total_data = self.total_success + self.total_failed
            self.log(crawling_time, id_project, project, sub_project, domain, sub_source, id_sub_src, total_data, self.total_success, self.total_failed, 'done', assign, 'F:/Work/Crawling Trust Pilot/log.json')