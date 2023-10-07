import scrapy
import csv
class EtsyShopSpiderSpider(scrapy.Spider):
    name = "etsy_shop_spider"
    allowed_domains = ["etsy.com"]
    start_urls = ["https://www.etsy.com/c?explicit=1&locationQuery=2542007"]
    def start_requests(self):
        # Charger les URL et ids des produits à partir du fichier CSV
        with open(r'C:\Users\Idrissa_TRAORE\PycharmProjects\etsy_project_attributes\etsy_scraping\etsy_scraping\all_attributes_final.csv',
                  'r', encoding='utf-8') as csvfile:
            # Charger notre fichier CSV
            reader = csv.reader(csvfile)
            # Ignorer la première ligne de l'en-tête
            next(reader)
            for row in reader:
                url = row[0]
                # Ajouter le schéma "http://" si absent
                if not url.startswith('http'):
                    url = 'http://' + url
                yield scrapy.Request(url=url, callback=self.parse, meta={'url': url})
    def parse(self, response):
        # Extraire les attributs des produits
        def clean_text(text):
            # Cette fonction permet de supprimer les espaces du début et de la fin d'une chaîne de caractères
            return text.strip() if text else None
        number_of_shop_sales = clean_text(response.xpath('//span[@class="wt-text-caption wt-no-wrap"]/text()').get())
        if number_of_shop_sales:
            number_of_shop_sales = number_of_shop_sales.replace(' Sales', '').replace(' Sale', '')

        shop_name = response.xpath('//h1[@class="wt-text-heading-01 wt-text-truncate"]/text()').get()

        type_of_products_in_shop = response.xpath(
            '//p[@class="wt-text-caption wt-hide-xs wt-show-lg wt-wrap wt-break-all"]/text()').get()

        shop_owner = response.xpath('//div[@class="img-container"]/a/p/text()').get()

        shop_exact_location = response.xpath('//span[@class="shop-location wt-text-caption wt-text-gray wt-line-height-tight wt-text-truncate"]/text()').get()

        shop_image_links = response.xpath('//img[@class="shop-icon-external wt-rounded wt-display-block wt-b-xs"]/@src | '
                                    '//img[@class="shop-icon-external wt-rounded wt-display-block wt-b-xs"]/@srcset').getall()
        shop_image_links = [link.replace('\n        ', '').strip() for link in shop_image_links if link]

        shop_description = response.xpath('//p[@class="wt-mt-xs-0 wt-text-gray announcement-collapse"]/span/text()').getall()
        shop_description = [desc.strip() for desc in shop_description]

        number_of_admirers = response.xpath('//div[@class="wt-mt-lg-5 wt-pt-lg-2 wt-bt-xs-1"]/div[2]/a/text()').get()
        if number_of_admirers:
            number_of_admirers = number_of_admirers.replace(' Admirers', '').replace(' Admirer', '')

        star_steller = clean_text(response.xpath('//p[@class="wt-text-caption-title wt-ml-xs-1"]/text()').get())

        number_of_stars = clean_text(response.xpath(
            '//span[@class="wt-display-inline-block wt-mr-xs-1"]/span[@class="wt-screen-reader-only"]/text()').get())
        if number_of_stars:
            number_of_stars = number_of_stars.replace(' out of 5 stars', '')

        # Créer un dictionnaire pour stocker les attributs extraits
        item = {
            'number_of_shop_sales': number_of_shop_sales,
            'star_steller': star_steller,
            'number_of_stars': number_of_stars,
            'shop_name': shop_name,
            'type_of_products_in_shop': type_of_products_in_shop,
            'shop_owner': shop_owner,
            'shop_exact_location': shop_exact_location,
            'shop_image_links': shop_image_links,
            'shop_description': shop_description,
            'number_of_admirers': number_of_admirers,
        }
        # Renvoyer l'objet Item avec les attributs extraits
        yield item

        # Enregistrement des résultats dans un fichier CSV
        with open('shop_attributes.csv', 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            # Écrire les en-têtes si le fichier est vide
            if csvfile.tell() == 0:
                writer.writerow(['Shop Link', 'Number Of Shop Sales','Star Steller','Number Of Stars', 'Shop Name', 'Type Of Products In Shop',
                                 'Shop Owner', 'Shop Exact Location', 'Shop Image Links', 'Shop Description',
                                 'Number Of Admirers'])

            writer.writerow([response.meta['url'], number_of_shop_sales, star_steller, number_of_stars, shop_name, type_of_products_in_shop,
                             shop_owner, shop_exact_location, shop_image_links, shop_description, number_of_admirers])
