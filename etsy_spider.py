import scrapy
import csv
class EtsySpider(scrapy.Spider):
    name = "etsy_spider"
    allowed_domains = ["etsy.com"]
    start_urls = ["https://www.etsy.com/c?explicit=1&locationQuery=2542007"]
    def start_requests(self):
        # Charger les URL et ids des produits à partir du fichier CSV
        with open(r'C:\Users\Idrissa_TRAORE\PycharmProjects\etsy_project_urls\urls_and_ids.csv', 'r') as csvfile:
            # Charger notre fichier CSV
            reader = csv.reader(csvfile)
            # Ignorer la première ligne de l'en-tête
            next(reader)
            for row in reader:
                url = row[0]
                id = row[1]
                # Ajouter le schéma "http://" si absent
                if not url.startswith('http'):
                    url = 'http://' + url
                yield scrapy.Request(url=url, callback=self.parse, meta={'url': url, 'id': id})
    def parse(self, response):
        #Extraire  les attributs des produits
        def clean_text(text):# Cette fonction permet de supprimer les espaces du début et de la fin d'une chaîne de caractères
            return text.strip() if text else None
        title = clean_text(response.xpath(
            '//h1[@class="wt-text-body-01 wt-line-height-tight wt-break-word wt-mt-xs-1"]/text()').get())
        item_location = clean_text(response.xpath(
            '//div[@class ="wt-grid__item-xs-12 wt-text-black wt-text-caption"]/text()').get())

        price = clean_text(response.xpath(
            '//p[@class="wt-text-title-03 wt-mr-xs-1"]/span[@class="wt-screen-reader-only"]/following-sibling::text()').get())

        shipment_preparation_time = clean_text(response.xpath('//p[@class="wt-text-caption"]/text()').get())

        returns_and_exchanges = clean_text(response.xpath(
            '//span[@class="wt-text-body-03 wt-mt-xs-1 wt-line-height-tight"]/text()').get())
        if returns_and_exchanges is None:
            returns_and_exchanges = clean_text(response.xpath(
                '//div[@class="wt-grid__item-xs-12 wt-pr-xs-2 wt-mb-md-5 wt-mb-xs-4"]/p[@class="wt-text-body-03 wt-mt-xs-1 wt-line-height-tight"]/text()').get())

        number_of_item_reviews = clean_text(
            response.xpath('//span[@class="wt-badge wt-badge--status-02 wt-ml-xs-2"]/text()').get())

        returns_and_exchanges_period = clean_text(response.xpath(
            '//span[contains(text(), "Return & exchange window")]/following-sibling::div//span/text()').get())

        gift_wrap = clean_text(response.xpath('//p[@class="wt-text-caption-title"]/text()').get())

        shipping_location = response.xpath('//span[@class="wt-flex-xs-auto wt-width-full"]/text()').get()

        details = response.xpath('//div[@id="product-details-content-toggle"]//li/div/text()').getall()
        details = [detail.strip() for detail in details]

        description = response.xpath('//div[@id="wt-content-toggle-product-details-read-more"]/p//text()').getall()
        description = [desc.strip() for desc in description]

        product_image_links = response.xpath(
            '//img[@class="wt-max-width-full wt-horizontal-center wt-vertical-center carousel-image wt-rounded"]/@src | '
            '//img[@class="wt-max-width-full wt-horizontal-center wt-vertical-center carousel-image wt-rounded"]/@srcset ').getall()
        image_high_resolution = response.xpath(
            '//img[@class="wt-max-width-full wt-horizontal-center wt-vertical-center carousel-image wt-rounded"]/@data-src-zoom-image').get()
        product_image_links.append(image_high_resolution)

        seller_id = response.xpath('//a/@data-to_user_id').get()

        shop_name = response.xpath('//a[@class="wt-text-link"]/text()')[1].get()
        shop_image_link = response.xpath('//div[@class="wt-thumbnail-larger wt-mr-xs-2"]/img/@src').get()
        shop_link = response.xpath('//p[@class=" wt-text-black wt-text-caption"]/a/@href').get()

        shipment_cost = ''.join(response.xpath(
            '//span[contains(text(), "Cost to ship")]/following-sibling::p//text()').getall())

        # dans 'view-source page'
        category = response.xpath(
            '//div[@class="wt-text-caption wt-text-center-xs wt-text-left-lg"]/a[last()-1]/text()').get()
        # [last()-1] sélectionne l'avant-dernier élément dans la liste

        order_placed_date = response.xpath(
            '//p[@class="wt-mt-xs-2 wt-text-black wt-text-caption-title wt-line-height-tight"]/text()').get()
        order_ships_dates = response.xpath(
            '//div[@class="wt-popover wt-display-flex-xs wt-flex-direction-column-xs wt-align-items-center"]/p[@class="wt-mt-xs-2 wt-text-black wt-text-caption-title wt-line-height-tight"]/text()').get()


        seller_name = response.xpath(
            '//p[@class="wt-text-body-03 wt-line-height-tight wt-mb-lg-1"]/text()').get()

        stock_availability = response.xpath(
            '//div[@class="wt-mb-xs-1 wt-mt-xs-1"]/p[@class="wt-text-title-01 wt-text-brick"]/text()').get()
        if stock_availability is None:
            stock_availability = " "

        estimate_arrival_date = response.xpath(
            '//p[@class="wt-text-body-03 wt-mt-xs-1 wt-line-height-tight"]/text()').get()

        number_of_shop_reviews = clean_text(response.css('div.wt-display-flex-xs > h2.wt-mr-xs-2::text').get())
        if number_of_shop_reviews:
            number_of_shop_reviews = number_of_shop_reviews.replace(' shop reviews', '').replace(' reviews', '')

        ########## Les selecteurs de 'sizes', 'colors' ou 'franges_tassels' varient selon les pages, par consequent, nous allons proceder ainsi: ##########
        def extract_options(response, keywords):
            # Rechercher tous les sélecteurs de variation sur la page
            variation_selectors = response.xpath('//select[@class="wt-select__element "]')
            # Extraire toutes les options disponibles de chaque sélecteur de variation qui contient un mot clé
            options = []
            for selector in variation_selectors:
                selector_text = selector.extract().lower()
                if any(keyword in selector_text for keyword in keywords):
                    options += selector.xpath('./option/text()').getall()
            # Supprimer les doublons et les éléments vides
            options = list(set([option.strip() for option in options if option.strip()]))
            # Supprimer l'option "Select an option" s'il est présent
            options = [option for option in options if option != "Select an option"]
            return options
            # Extraire les tailles
        sizes = extract_options(response, ["size"])
        # Extraire les couleurs
        colors = extract_options(response,
                                 ["color", "charcoal", "navy", "dark chocolate", "white", "black", "red", "purple"])
        # Extraire les fringes et les tassels
        fringes_tassels = extract_options(response, ["fringes", "tassels"])

        # Maintenant, nous recuperons les attributs manquants

        categories = response.xpath(
        '//div[@class="wt-text-caption wt-text-centerh -xs wt-text-left-lg"]/a/text()').getall()
        categories = " > ".join(categories)

        shop_id = response.xpath(
            '//a[@class="inline-overlay-trigger favorite-shop-action wt-btn wt-btn--small wt-btn--transparent follow-shop-button-listing-header-v3 wt-btn--transparent-flush-left"]/@data-shop-id').get()

        taxonomy_id = response.xpath(
            '//div[contains(@data-appears-component-name, "Listzilla_ApiSpecs_SameShop")]/@data-appears-event-data') \
            .re_first(r'"taxonomy_ids":\[(\d+)')

        quantity = response.xpath('//script[contains(text(), "eligibleQuantity")]/text()').re_first(
            r'"eligibleQuantity":(\d+)')

        tags = response.xpath('//script[contains(text(), "listing_tags")]/text()').re_first(
            r'"listing_tags":\[(.*?)\]')

        sku = response.xpath('//script[contains(text(), "sku")]/text()').re_first(r'"sku":"(\d+)"')

    # Créer un dictionnaire pour stocker les attributs extraits
        item = {
            'title': title,
            'item_location': item_location,
            'shipping_location': shipping_location,
            'price': price,
            'category': category,
            'stock_availability': stock_availability,
            'shipment_preparation_time': shipment_preparation_time,
            'gift_wrap': gift_wrap,
            'order_placed_date': order_placed_date,
            'order_ships_dates': order_ships_dates,
            'estimate_arrival_date': estimate_arrival_date,
            'details': details,
            'description': description,
            'product_image_links': product_image_links,
            'seller_name': seller_name,
            'seller_id': seller_id,
            'shop_name': shop_name,
            'shop_image_link': shop_image_link,
            'shop_link': shop_link,
            'number_of_shop_reviews': number_of_shop_reviews,
            'number_of_item_reviews': number_of_item_reviews,
            'returns_and_exchanges': returns_and_exchanges,
            'returns_and_exchanges_period': returns_and_exchanges_period,
            'shipment_cost': shipment_cost,
            'sizes': sizes,
            'fringes_tassels': fringes_tassels,
            'colors': colors,
            'shop_id': shop_id,
            'categories' : categories,
            'taxonomy_id': taxonomy_id,
            'quantity': quantity,
            'tags':tags,
            'sku': sku

        }
        # Renvoyer l'objet Item avec les attributs extraits
        yield item

        # Enregistrement des résultats dans un fichier CSV
        with open('all_attributes.csv', 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            # Écrire les en-têtes si le fichier est vide
            if csvfile.tell() == 0:
                writer.writerow(['Url','Id','Title', 'Item Location', 'Shipping Location', 'Price', 'Category', 'Stock Availability',
                                 'Shipment Preparation Time', 'Gift Wrap', 'Order Placed Date',
                                 'Order Ships Dates', 'Estimate Arrival Date', 'Details', 'Description',
                                 'Product Image Links', 'Seller Name', 'Seller ID', 'Shop Name', 'Shop Image Link', 'Shop Link',
                                 'Number of Shop Reviews', 'Number of Item Reviews', 'Returns and Exchanges',
                                 'Returns and Exchanges Period', 'Shipment Cost', 'Sizes', 'Fringes/Tassels', 'Colors', 'Categories','Taxonomy id', 'Quantity', 'Tags', 'Sku'])
            writer.writerow([response.meta['url'], response.meta['id'], title, item_location, shipping_location, price, category,
                             stock_availability, shipment_preparation_time, gift_wrap, order_placed_date, order_ships_dates,
                             estimate_arrival_date, details, description, product_image_links, seller_name, seller_id,
                             shop_name, shop_image_link, shop_link, number_of_shop_reviews, number_of_item_reviews,
                             returns_and_exchanges, returns_and_exchanges_period, shipment_cost, sizes, fringes_tassels,
                             colors, shop_id, categories, taxonomy_id, quantity, tags, sku])
