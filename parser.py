from selenium import webdriver
import time
from bs4 import BeautifulSoup
import pickle
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import csv
import pandas as pd
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
import os
import logging
from preproc import preprocessing_result
from dotenv import load_dotenv


# Подключаем переменные окружения
load_dotenv()

# Настройки, отвечающие за выключение графического интерфейса (нужны для работы на сервере)
options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.set_capability('goog:loggingPrefs', {'browser': 'ALL'})

service = ChromeService(ChromeDriverManager().install())

browser = webdriver.Chrome(service=service,
                           options=options
                           )

# Флаг, отвечающий за корректную загрузку последней страницы. Принимаем значение 1 в случае, когда последняя страница
# не отображает грузы
flag = 0

# Счетчик количества итераций парсера
iter_counter = 0

# Настройка базовых конфигураций логов
def configure_logging(level: int) -> None:
    logging.basicConfig(
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S",
        format="[%(asctime)s.%(msecs)03d] %(module)10s:%(lineno)-3d %(levelname)-7s - %(message)s",
    )


logger = logging.getLogger(__name__)

configure_logging(level=logging.INFO)


# Функция для авторизации на сайте ати
def login(email: str, password: str) -> int | None:
    logger.info("Начало авторизации")

    # Заходим на сайт для авторизации
    try:
        browser.get("https://id.ati.su/login/?next=https%3A%2F%2Fati.su%2F")
        # Ожидание, пока загрузится элемент
        wait = WebDriverWait(browser, 30)
        wait.until(EC.presence_of_element_located(
            (By.XPATH, '//button[@class="glz-button glz-is-primary login-control-button"]')))
    except TimeoutException:
        # Перезагружаем страницу
        browser.refresh()
        try:
            browser.get("https://id.ati.su/login/?next=https%3A%2F%2Fati.su%2F")
            # Ожидание, пока загрузится элемент
            wait = WebDriverWait(browser, 30)
            wait.until(EC.presence_of_element_located(
                (By.XPATH, '//button[@class="glz-button glz-is-primary login-control-button"]')))
        except TimeoutException:
            # Если не удалось загрузить страницу, то переходим к следующей итерации цикла
            logger.warning("Не удалось загрузить страницу для начала авторизации")
            return -1

    # Получение высоты страницы
    total_height = browser.execute_script("return document.body.scrollHeight")

    # Изменение размера окна браузера до полного размера страницы
    browser.set_window_size(1920, total_height)

    # Авторизация
    try:
        browser.find_element("xpath", '//input[@id="login"]').send_keys(email)
        browser.find_element("xpath", '//input[@id="password"]').send_keys(password)
        browser.find_element("xpath",
                             '//button[@class="glz-button glz-is-primary login-control-button"]').click()
    except NoSuchElementException:
        logger.error("Не удалось выполнить авторизацию")

    # Ожидание загрузки страницы
    try:
        # Ожидание, пока загрузится элемент
        wait = WebDriverWait(browser, 5)
        wait.until(EC.presence_of_element_located(
            (By.XPATH, "//button[@class='glz-button glz-is-medium glz-is-secondary ZNE8y-0-1-11']")))
    except TimeoutException:
        browser.refresh()

    logger.info("Конец авторизации")


# Функция загрузки куки файлов
def ati_cookie():
    logger.info("Начало загрузки куки")

    # Сохранение куки в файл ati_cookies
    pickle.dump(browser.get_cookies(), open("ati_cookies", "wb"))
    for cookie in pickle.load(open("ati_cookies", "rb")):
        browser.add_cookie(cookie)

    logger.info("Конец загрузки куки\n")


# Функция для создания директорий
def create_dir(name: str) -> None:
    try:
        os.makedirs(name)
        logger.info('Папка %s успешно создана.', name)
    except FileExistsError:
        logger.info('Папка %s уже существует.', name)
    except Exception as e:
        logger.warning('При создании папки произошла ошибка: %s', e, exc_info=True)


# Функция, которая получает количество страниц, необходимое для парсинга (нужно для пагинации)
def get_number_of_pages(route: str) -> int:
    logger.info("Начало получения количества страниц")
    browser.get(route)

    # time.sleep(120)

    # Ждем, пока страница полностью прогрузится
    try:
        wait = WebDriverWait(browser, 30)
        wait.until(EC.presence_of_element_located(
            (By.XPATH, "//button[@class='XfMtd WYYP5']")))

    except TimeoutException:
        logger.warning("Не удалось загрузить сайт для получения количества страниц, пробуем обновить страницу")
        try:
            try:
                # Проверяем есть ли вообще подходящие грузы
                absence_cargo = browser.find_element('xpath',
                                                     "//p[@class='glz-h glz-is-size-4 EmptyResults_title__O_s31']")
                if absence_cargo.text == 'Подходящих грузов не нашлось':
                    return -2
            except NoSuchElementException:
                # Удаление всех куки
                browser.delete_all_cookies()
                # Повторная авторизация на сайте
                if login(os.getenv('ATI_LOGIN'), os.getenv('ATI_PASSWORD')) == -1:
                    return -1
                # Загрузка куки
                ati_cookie()
                # Заходим на страницу с грузами
                browser.get(route)
                # Перезагружаем страницу
                browser.refresh()
                # Прокручиваем страницу вниз
                browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                # # Ждем, пока кнопка станет доступной для нажатия
                # button_to_click = wait.until(EC.element_to_be_clickable(
                #     (By.XPATH,
                #      "//button[@class='Sticky_actionButton__KuldF glz-button glz-is-primary glz-is-outlined glz-is-small']")))
                # button_to_click.click()  # Нажимаем на кнопку
                wait = WebDriverWait(browser, 30)
                wait.until(EC.presence_of_element_located(
                    (By.XPATH, "//button[@class='XfMtd WYYP5']")))
        except TimeoutException:
            print(browser.find_element('xpath', "//p[@class='glz-h glz-is-size-4 EmptyResults_title__O_s31']"))
            # Если не удалось получить количество страниц спустя две попытки, то прерываем итерацию программы
            return -1

    pages_count = browser.find_elements('xpath', "//BUTTON[@class='total-index_kjYkG']")

    # Массив array_pag содержит информацию о максимальном элементе пагинации
    # (работает, когда страниц > 5, так как парсит максимальное значение пагинации)
    array_pag = []
    for i in pages_count:
        array_pag.append(i.text)
    page_number = 0

    # Если количество страниц меньше 5, то находим максимально значение
    # (не работает для одной страницы, так как у нее отсутствует пагинация)
    if not array_pag:
        counter = browser.find_elements('xpath', "//button[@class='item_JqfSO']")
        if not counter:
            # Краевой случай, когда нашлась только 1 страница и нет пагинации
            # (проверяем элемент, который содержит информацию о том, сколько грузов найдено)
            text_number = browser.find_element('xpath',
                                               "//h2[@class='SearchResults_totalItems__6GSjj glz-h glz-is-size-2']")
            text_number = text_number.text
            for i in range(len(text_number)):
                # Если количество найденных грузов больше 0, значит будем производить парсинг страницы
                if (text_number[i].isnumeric()) and (text_number[i].isnumeric() > 0):
                    page_number = 1
        else:
            # Проходим по найденным элементам пагинации и берем максимальный
            for el in counter:
                if len(el.text) != 0:
                    page_number = max(page_number, int(el.text))

    # Извлекаем количество страниц пагинации (работает, когда страниц больше 5)
    else:
        for i in array_pag:
            if len(i) != 0:
                page_number = int(i)
                break


    logger.info("Конец получения количества страниц")
    logger.info("Количество найденных страниц: %s", page_number)
    return page_number


# Функция сохранения страниц
def save_pages(n_route: int, route: str) -> int:
    logger.info("Начало сохранения страниц")

    # Получаем количество страниц
    right = get_number_of_pages(route)

    # Делаем проверку на то, что количество страниц было найдено и сайт удалось отобразить корректно. В случае какой-то
    # ошибки прерываем работу программы.
    if right == -1:
        return right
    elif right == -2:
        return right

    # Выполняем загрузку страниц
    for i in range(0, right + 1):

        # Ожидание загрузки страницы
        wait = WebDriverWait(browser, 30)
        # Ожидание, пока загрузится элемент (многоточие справа снизу)
        wait.until(EC.presence_of_element_located((By.XPATH, "//button[@class='XfMtd WYYP5']")))
        if i > 1:
            browser.find_elements("xpath",
                                  f"//button[@class='item_JqfSO' and @data-value='{i}']")[-1].click()
        try:
            # Выполняем проверку на то, что на загруженной странице отображаются грузы
            # (иногда сайт лагает и при переходе на страницу грузов нет)
            wait = WebDriverWait(browser, 5)
            wait.until(EC.presence_of_element_located((By.XPATH, "//button[@class='XfMtd WYYP5']")))
        except TimeoutException:
            # В случае, когда сайт залагал и грузы не отображаются поднимаем флаг и выходим из цикла
            # (обычно это происходит на последней странице, так как на сайте криво отображаются грузы)
            global flag
            flag = 1
            break
        try:
            # У некоторых заказов для получения контактной информации необходимо нажать на кнопку "Показать контакты"
            # Ждем загрузку таких элементов и нажимаем на кнопку при необходимости
            wait = WebDriverWait(browser, 3)
            wait.until(EC.presence_of_element_located((By.XPATH,
                                                       "//button[@class='glz-is-primary glz-is-pointer glz-is-outlined"
                                                       " glz-button glz-is-small']")))

            contacts = browser.find_elements("xpath",
                                             "//button[@class='glz-is-primary glz-is-pointer glz-is-outlined "
                                             "glz-button glz-is-small']")
            if len(contacts) > 0:
                for j in contacts:
                    j.click()
        except:
            pass

        # Скачиваем каждую страницу
        with open(f"pages/{i}_page_ati_route{n_route}.html", 'w', encoding='utf-8') as file:
            file.write(browser.page_source)

    logger.info("Конец сохранения страниц")

    return right


# Парсер информации со скаченных пдф страниц
def pdf_pars(n_route: int, number_of_page: int) -> None:
    logger.info("Начало парсинга скачанных страниц")

    # Открываем файл для записи
    with open(f'orders_route{n_route}.csv', 'w', newline='', encoding='utf-8') as csvfile:
        # Создаем объект writer
        writer = csv.writer(csvfile, delimiter=';')

        # Записываем заголовки
        writer.writerow(['Number_of_cargo', 'Distance', 'Transport', 'Cargo',
                         'Start', 'End', 'Price', 'Contacts', 'Link'])
        end = number_of_page
        global flag
        # Если флаг поднят, то значит последняя страница не отображается корректно, значит нужно идти до N-1 страницы
        if flag == 1:
            end = end - 1
        for j in range(0, end + 1):

            # Считываем страничку и создаем парсер
            with open(f"pages/{j}_page_ati_route{n_route}.html", encoding='utf-8') as file:
                src = file.read()
            soup = BeautifulSoup(src, 'lxml')

            # Собираем информацию о кол-ве грузов на странице (на сайте написано, что отображаются по 10, но это ложь)
            orders = soup.find_all(attrs={'data-app': 'pretty-load'})

            # Проходимся циклом по элементам заказа
            for i, item in enumerate(orders, start=1):
                # Номер перевозки
                try:
                    item_number = item.find(class_='DAoTb').text.strip()  # //span[@class='DAoTb']
                except NoSuchElementException:
                    item_number = 'Информация недоступна'
                # Расстояние
                try:
                    item_distance = item.find(class_='Laof2').text.strip()
                except NoSuchElementException:
                    item_distance = 'Информация недоступна'
                # Вид машины
                try:
                    item_transport = item.find(class_='qJ9Xx').text.strip()
                except NoSuchElementException:
                    item_transport = 'Информация недоступна'
                # Информация о грузе
                try:
                    item_gruz = item.find(class_='tk4l9').text.strip()
                except NoSuchElementException:
                    item_gruz = 'Информация недоступна'
                # Начало перевозки
                try:
                    item_start = [elem.text.strip() for elem in item.find_all(class_='BUBXM h56vj')]
                    if len(item_start) == 0:
                        item_start = [elem.text.strip() for elem in item.find_all(class_='yLMmR')]
                except NoSuchElementException:
                    item_start = 'Информация недоступна'
                # Конец перевозки
                try:
                    item_end = [elem.text.strip() for elem in item.find_all(class_='BUBXM')]
                except NoSuchElementException:
                    item_end = 'Информация недоступна'
                # Информация о стоимости
                try:
                    item_price = item.find(class_='Pai6y').text.strip()
                except NoSuchElementException:
                    item_price = 'Информация недоступна'
                # Контактная информация
                try:
                    item_contacts = item.find(class_='mBazR').text.strip()
                except NoSuchElementException:
                    item_contacts = 'Информация недоступна'
                # Ссылка на груз
                try:
                    item_link = item.find(class_='XfMtd fmVZ6').get('href')
                except NoSuchElementException:
                    item_link = 'Информация недоступна'

                writer.writerow([item_number, item_distance, item_transport,
                                 item_gruz, item_start, item_end,
                                 item_price, item_contacts, item_link])

    logger.info("Конец парсинга скачанных страниц\n")


# Основная функция запуска парсера
def parser_main():
    # Запускаем долгий цикл, рассчитанный примерно на 12 часов непрерывной работы
    while True:
        global flag, sp, iter_counter
        logger.info("Старт %s итерации парсера", iter_counter)
        # Отдельно обрабатываем первую итерацию парсинга. При первой итерации программа обрабатывает грузы,
        # которые выложены за весь временной промежуток. Делается это из расчета на то, что программа в первый раз
        # может быть запущена спустя какой-то промежуток времени, поэтому нужно обработать все имеющиеся на данный
        # момент перевозки.
        if iter_counter == 0:
            start_time = time.time()
            list_of_route = [
                'https://loads.ati.su/?utm_source=header&utm_campaign=new_header&_gl=1*hksq9c*_gcl_au*MTI3MTU5OTUyNi4x'
                'NzA5MDA5ODE4&_ga=2.140807666.1988261297.1710654201-1362612257.1701233591#?filter=%7B%22from%22:%22%D0'
                '%9A%D1%80%D0%B0%D1%81%D0%BD%D0%BE%D1%8F%D1%80%D1%81%D0%BA,%20%D0%A0%D0%A4%22,%22fromGeo%22:%222_116%2'
                '2,%22fromGeo_tmp%22:%221_41%22,%22fromRadius%22:50,%22exactFromGeos%22:true,%22to%22:%22%D0%90%D0%B1%'
                'D0%B0%D0%BA%D0%B0%D0%BD,%20%D0%A0%D0%A4%22,%22toGeo%22:%222_3056%22,%22toGeo_tmp%22:%221_57%22,%22toR'
                'adius%22:50,%22exactToGeos%22:true,%22volume%22:%7B%22to%22:90%7D,%22weight%22:%7B%22to%22:20%7D,%22f'
                'irmListsExclusiveMode%22:false,%22dateOption%22:%22today-tomorrow%22,%22truckType%22:%221%22,%22loadi'
                'ngType%22:%2215%22,%22withAuction%22:false%7D',

                'https://loads.ati.su/?utm_source=header&utm_campaign=new_header&_gl=1*1w4eoa*_gcl_au*MTI3MTU5OTUyNi4x'
                'NzA5MDA5ODE4&_ga=2.161884668.1988261297.1710654201-1362612257.1701233591#?filter=%7B%22from%22:%22%D0'
                '%90%D0%B1%D0%B0%D0%BA%D0%B0%D0%BD,%20%D0%A0%D0%A4%22,%22fromGeo%22:%222_3056%22,%22fromGeo_tmp%22:%22'
                '1_57%22,%22fromRadius%22:50,%22exactFromGeos%22:true,%22to%22:%22%D0%9D%D0%BE%D0%B2%D0%BE%D1%81%D0%B8'
                '%D0%B1%D0%B8%D1%80%D1%81%D0%BA,%20%D0%A0%D0%A4%22,%22toGeo%22:%222_170%22,%22toGeo_tmp%22:%221_80%22,'
                '%22toRadius%22:50,%22exactToGeos%22:true,%22volume%22:%7B%22to%22:90%7D,%22weight%22:%7B%22to%22:20%7'
                'D,%22firmListsExclusiveMode%22:false,%22truckType%22:%221%22,%22loadingType%22:%2215%22,%22withAuctio'
                'n%22:false%7D',

                # 'https://loads.ati.su/?utm_source=header&utm_campaign=new_header&_gl=1*1w4eoa*_gcl_au*MTI3MTU5OTUyNi4x'
                # 'NzA5MDA5ODE4&_ga=2.161884668.1988261297.1710654201-1362612257.1701233591#?filter=%7B%22from%22:%22%D0'
                # '%90%D0%B1%D0%B0%D0%BA%D0%B0%D0%BD,%20%D0%A0%D0%A4%22,%22fromGeo%22:%222_3056%22,%22fromGeo_tmp%22:%22'
                # '1_57%22,%22fromRadius%22:50,%22exactFromGeos%22:true,%22to%22:%22%D0%9D%D0%BE%D0%B2%D0%BE%D1%81%D0%B8'
                # '%D0%B1%D0%B8%D1%80%D1%81%D0%BA,%20%D0%A0%D0%A4%22,%22toGeo%22:%222_170%22,%22toGeo_tmp%22:%221_80%22,'
                # '%22toRadius%22:50,%22exactToGeos%22:true,%22volume%22:%7B%22to%22:90%7D,%22weight%22:%7B%22to%22:20%7'
                # 'D,%22firmListsExclusiveMode%22:false,%22truckType%22:%221%22,%22loadingType%22:%2215%22,%22withAuctio'
                # 'n%22:false%7D'

                'https://loads.ati.su/?utm_source=header&utm_campaign=new_header&_gl=1*4l6352*_gcl_au*MTI3MTU5OTUyNi4x'
                'NzA5MDA5ODE4&_ga=2.129460684.1988261297.1710654201-1362612257.1701233591#?filter=%7B%22from%22:%22%D0'
                '%9D%D0%BE%D0%B2%D0%BE%D1%81%D0%B8%D0%B1%D0%B8%D1%80%D1%81%D0%BA,%20%D0%A0%D0%A4%22,%22fromGeo%22:%222'
                '_170%22,%22fromGeo_tmp%22:%221_80%22,%22fromRadius%22:50,%22exactFromGeos%22:true,%22to%22:%22%D0%9A%'
                'D1%80%D0%B0%D1%81%D0%BD%D0%BE%D1%8F%D1%80%D1%81%D0%BA,%20%D0%A0%D0%A4%22,%22toGeo%22:%222_116%22,%22t'
                'oGeo_tmp%22:%221_41%22,%22toRadius%22:50,%22exactToGeos%22:true,%22volume%22:%7B%22to%22:90%7D,%22wei'
                'ght%22:%7B%22to%22:20%7D,%22firmListsExclusiveMode%22:false,%22truckType%22:%221%22,%22loadingType%22'
                ':%2215%22,%22withAuction%22:false%7D'
            ]

            # Авторизация на сайте
            # Если не удалось загрузить страницу, то переходим к следующей итерации цикла
            if login(os.getenv('ATI_LOGIN'), os.getenv('ATI_PASSWORD')) == -1:
                continue
            # Загрузка куки
            ati_cookie()

            create_dir('pages')

            # Проходимся по маршрутам и выполняем парсинг
            for n_route, route in enumerate(list_of_route):
                sp = save_pages(n_route, route)
                if sp == -2:
                    logger.info('Подходящих грузов не нашлось\n')
                    continue
                elif sp == -1:
                    logger.warning("Не удалось загрузить сайт для получения количества страниц после обновления\n")
                    break
                pdf_pars(n_route, sp)
                flag = 0

            if sp == -1:
                logger.warning("Принудительное завершение итерации парсера")
                continue

            df1 = pd.read_csv('orders_route0.csv', delimiter=';', encoding='utf-8')
            df2 = pd.read_csv('orders_route1.csv', delimiter=';', encoding='utf-8')
            df3 = pd.read_csv('orders_route2.csv', delimiter=';', encoding='utf-8')

            # Полученная информация собирается в единый датасет
            new_df = pd.concat([df1, df2, df3], ignore_index=True)
            # new_df = df1.copy()

            # new_df.to_excel(f"data/server_orders_{datetime.datetime.now().strftime('%d_%m(%H.%M)')}v_test.xlsx",
            #                 index=False)

            create_dir('test_data')

            new_df.to_excel(f"test_data/server_orders_test.xlsx", index=False)
            end_time = time.time()
            execution_time = end_time - start_time
            logger.info("Время выполнения: %s секунд", execution_time)
            logger.info("Конец %s итерации парсера\n", iter_counter)

            # Функция из файла preproc.py, которая выполняет обработку данных и загрузку их в бд
            preprocessing_result()
            iter_counter += 1
            time.sleep(120)

            # break

        # Все последующие итерации работы программы выполняются для самых свежих грузов (за последний час)
        else:
            # time.sleep(120)
            # time.sleep(15)
            start_time = time.time()
            list_of_route = [
                'https://loads.ati.su/?utm_source=header&utm_campaign=new_header&_gl=1*hksq9c*_gcl_au*MTI3MTU5OTUyNi4x'
                'NzA5MDA5ODE4&_ga=2.140807666.1988261297.1710654201-1362612257.1701233591#?filter=%7B%22from%22:%22%D0'
                '%9A%D1%80%D0%B0%D1%81%D0%BD%D0%BE%D1%8F%D1%80%D1%81%D0%BA,%20%D0%A0%D0%A4%22,%22fromGeo%22:%222_116%2'
                '2,%22fromGeo_tmp%22:%221_41%22,%22fromRadius%22:50,%22exactFromGeos%22:true,%22to%22:%22%D0%90%D0%B1%'
                'D0%B0%D0%BA%D0%B0%D0%BD,%20%D0%A0%D0%A4%22,%22toGeo%22:%222_3056%22,%22toGeo_tmp%22:%221_57%22,%22toR'
                'adius%22:50,%22exactToGeos%22:true,%22volume%22:%7B%22to%22:90%7D,%22weight%22:%7B%22to%22:20%7D,%22f'
                'irmListsExclusiveMode%22:false,%22dateOption%22:%22today-tomorrow%22,%22truckType%22:%221%22,%22loadi'
                'ngType%22:%2215%22,%22changeDate%22:1,%22withAuction%22:false%7D',

                'https://loads.ati.su/?utm_source=header&utm_campaign=new_header&_gl=1*1w4eoa*_gcl_au*MTI3MTU5OTUyNi4x'
                'NzA5MDA5ODE4&_ga=2.161884668.1988261297.1710654201-1362612257.1701233591#?filter=%7B%22from%22:%22%D0'
                '%90%D0%B1%D0%B0%D0%BA%D0%B0%D0%BD,%20%D0%A0%D0%A4%22,%22fromGeo%22:%222_3056%22,%22fromGeo_tmp%22:%22'
                '1_57%22,%22fromRadius%22:50,%22exactFromGeos%22:true,%22to%22:%22%D0%9D%D0%BE%D0%B2%D0%BE%D1%81%D0%B8'
                '%D0%B1%D0%B8%D1%80%D1%81%D0%BA,%20%D0%A0%D0%A4%22,%22toGeo%22:%222_170%22,%22toGeo_tmp%22:%221_80%22,'
                '%22toRadius%22:50,%22exactToGeos%22:true,%22volume%22:%7B%22to%22:90%7D,%22weight%22:%7B%22to%22:20%7'
                'D,%22firmListsExclusiveMode%22:false,%22truckType%22:%221%22,%22loadingType%22:%2215%22,%22changeDate'
                '%22:1,%22withAuction%22:false%7D',

                'https://loads.ati.su/?utm_source=header&utm_campaign=new_header&_gl=1*4l6352*_gcl_au*MTI3MTU5OTUyNi4x'
                'NzA5MDA5ODE4&_ga=2.129460684.1988261297.1710654201-1362612257.1701233591#?filter=%7B%22from%22:%22%D0'
                '%9D%D0%BE%D0%B2%D0%BE%D1%81%D0%B8%D0%B1%D0%B8%D1%80%D1%81%D0%BA,%20%D0%A0%D0%A4%22,%22fromGeo%22:%222'
                '_170%22,%22fromGeo_tmp%22:%221_80%22,%22fromRadius%22:50,%22exactFromGeos%22:true,%22to%22:%22%D0%9A%'
                'D1%80%D0%B0%D1%81%D0%BD%D0%BE%D1%8F%D1%80%D1%81%D0%BA,%20%D0%A0%D0%A4%22,%22toGeo%22:%222_116%22,%22t'
                'oGeo_tmp%22:%221_41%22,%22toRadius%22:50,%22exactToGeos%22:true,%22volume%22:%7B%22to%22:90%7D,%22wei'
                'ght%22:%7B%22to%22:20%7D,%22firmListsExclusiveMode%22:false,%22truckType%22:%221%22,%22loadingType%22'
                ':%2215%22,%22changeDate%22:1,%22withAuction%22:false%7D'
            ]

            # Проходимся по маршрутам и выполняем парсинг
            for n_route, route in enumerate(list_of_route):
                sp = save_pages(n_route, route)
                if sp == -2:
                    logger.info('Подходящих грузов не нашлось\n')
                    continue
                elif sp == -1:
                    logger.warning("Не удалось загрузить сайт для получения количества страниц после обновления\n")
                    break
                pdf_pars(n_route, sp)
                flag = 0

            if sp == -1:
                logger.warning("Принудительное завершение итерации парсера")
                continue

            df1 = pd.read_csv('orders_route0.csv', delimiter=';', encoding='utf-8')
            df2 = pd.read_csv('orders_route1.csv', delimiter=';', encoding='utf-8')
            df3 = pd.read_csv('orders_route2.csv', delimiter=';', encoding='utf-8')

            new_df = pd.concat([df1, df2, df3], ignore_index=True)
            # new_df = pd.concat([df1, df2], ignore_index=True)

            # new_df.to_excel(f"data/server_orders_{datetime.datetime.now().strftime('%d_%m(%H.%M)')}v3.xlsx",
            #                 index=False)
            new_df.to_excel(f"test_data/server_orders_test.xlsx",
                            index=False)

            end_time = time.time()
            execution_time = end_time - start_time
            logger.info("Время выполнения: %s секунд", execution_time)
            logger.info("Конец %s итерации парсера\n", iter_counter)

            preprocessing_result()
            iter_counter += 1
            time.sleep(120)
            # time.sleep(20)


if __name__ == '__main__':
    # start_time = time.time()
    parser_main()
    # end_time = time.time()
    # execution_time = end_time - start_time
    # print(f"Время выполнения: {execution_time} секунд")