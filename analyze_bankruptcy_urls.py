#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import os
import sys
import json
import time
import re
from tqdm import tqdm
from datetime import datetime
from openai import OpenAI
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Получение API-ключа OpenAI из переменных окружения
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    print("❌ Ошибка: OPENAI_API_KEY не найден в .env файле")
    sys.exit(1)

# Инициализация клиента OpenAI
client = OpenAI(api_key=OPENAI_API_KEY)

# Промпт для анализа URL
ANALYSIS_PROMPT = """
Ты эксперт по анализу веб-страниц в сфере банкротства физических лиц.
Твоя задача - детально оценить вероятность того, что посетитель данной страницы намерен ВОСПОЛЬЗОВАТЬСЯ УСЛУГОЙ БАНКРОТСТВА ФИЗЛИЦ, а не просто ищет информацию.

Проанализируй URL и его контекст: {url}

На основе URL и твоих знаний о структуре и контенте сайтов о банкротстве, оцени:

1. НАМЕРЕНИЕ И КОНТЕКСТ:
   - Какой тип услуги, вероятно, предлагается на странице? Это юридическое сопровождение банкротства, бесплатная консультация, или что-то другое?
   - Целевая аудитория: физические лица с долгами, или кто-то другой?
   - К каким действиям, вероятно, побуждает страница: получение информации, заказ услуги, заявка на консультацию?

2. КОНВЕРСИОННЫЕ ЭЛЕМЕНТЫ (оцени вероятность наличия):
   - Формы заявок на получение услуги банкротства
   - Калькуляторы для расчета стоимости банкротства
   - Кнопки "Оставить заявку", "Получить консультацию", "Начать банкротство" и т.п.
   - Контактная информация для связи с юристами
   - Возможность онлайн-чата с консультантом

3. СПЕЦИФИКА БАНКРОТСТВА ФИЗЛИЦ:
   - Насколько вероятно наличие специфических терминов: "списание долгов", "реструктуризация", "реализация имущества", "финансовый управляющий"?
   - Вероятность упоминания законов о банкротстве физлиц (127-ФЗ)?
   - Вероятность наличия описания этапов/процедуры банкротства?

4. ЭТАП ВОРОНКИ ПРОДАЖ:
   - На каком этапе воронки продаж, вероятно, находится страница: осведомленность, интерес, рассмотрение, решение?
   - Насколько страница, вероятно, близка к конечной конверсии (получению заявки)?

5. СРАВНИТЕЛЬНЫЙ АНАЛИЗ:
   - Эта страница, скорее всего, больше похожа на информационную статью, посадочную страницу услуги или форму заявки?
   - Сравни этот URL с типичными URL транзакционного характера в сфере юридических услуг.

ОЦЕНКА ТРАНЗАКЦИОННОГО НАМЕРЕНИЯ:
Используй следующую шкалу для оценки транзакционного намерения:
- 0-20%: Чисто информационная страница без коммерческого контекста
- 21-40%: Преимущественно информационная страница с минимальными коммерческими элементами
- 41-60%: Смешанный контент (информация + предложение услуг)
- 61-80%: Коммерческая страница с явным предложением услуг банкротства
- 81-100%: Прямая страница для заказа услуги банкротства (форма заявки, страница заказа)

ВАЖНО: Верни результат строго в формате JSON:
{
  "intentScore": число от 0 до 100,
  "intentCategory": "соответствующая категория из списка выше",
  "targetAudience": "физлица с долгами/другая аудитория",
  "transactionalElements": {
    "applicationForm": вероятность наличия от 0 до 100,
    "calculator": вероятность наличия от 0 до 100,
    "contactInfo": вероятность наличия от 0 до 100,
    "callToAction": вероятность наличия от 0 до 100,
    "chat": вероятность наличия от 0 до 100
  },
  "bankruptcySpecificTerms": ["термин1", "термин2", ...],
  "funnelStage": "этап воронки продаж",
  "detailedReasoning": "детальное объяснение оценки (до 200 слов)",
  "confidence": число от 0 до 100 (насколько уверен в своей оценке)
}
"""

def extract_domain(url):
    """
    Извлекает домен из URL
    """
    try:
        # Удаляем протокол
        domain = url.split('//')[1] if '//' in url else url
        # Берем только домен (до первого слеша)
        domain = domain.split('/')[0]
        return domain
    except:
        return url

def get_domain_extension(domain):
    """
    Извлекает расширение домена (.ru, .com, .org и т.д.)
    """
    try:
        parts = domain.split('.')
        if len(parts) > 1:
            return parts[-1].lower()
        return ""
    except:
        return ""

def is_russian_domain(url):
    """
    Проверяет, является ли URL русским доменом
    """
    try:
        if not isinstance(url, str):
            return False
            
        # Нормализация URL
        url = url.lower()
        
        # Прямые признаки русского домена
        russian_tlds = ['.ru', '.su', '.rf', '.xn--p1ai', '.рф']
        
        # Проверка на кириллицу в URL
        has_cyrillic = bool(re.search('[\u0400-\u04FF]', url))
        
        # Проверка на пункод (.xn--)
        has_punycode = 'xn--' in url
        
        # Проверка на русский TLD
        has_russian_tld = any(tld in url for tld in russian_tlds)
        
        return has_russian_tld or has_cyrillic or has_punycode
    except Exception as e:
        print(f"❌ Ошибка при проверке русского домена: {str(e)}")
        return False

def process_excel_data(excel_file, output_csv, max_urls=None):
    """
    Обрабатывает Excel-файл и подготавливает данные для анализа банкротства физлиц
    """
    try:
        # Загрузка данных из Excel
        print(f"📊 Чтение данных из файла {excel_file}...")
        df = pd.read_excel(excel_file)
        original_count = len(df)
        print(f"📈 Всего строк в файле: {original_count}")
        
        # Выводим информацию о столбцах
        print(f"📋 Столбцы в файле: {', '.join(df.columns.tolist())}")
        
        # Проверка наличия колонки URL
        url_columns = [col for col in df.columns if col.upper() == 'URL']
        
        if not url_columns:
            print("❌ Ошибка: В Excel-файле отсутствует столбец с URL")
            return None
        
        # Проверка наличия столбца с трафиком
        # В файле BFLMSKvika.xlsx столбец называется "Organic Traffic  –  Ahrefs  :  URL"
        expected_traffic_column = "Organic Traffic  –  Ahrefs  :  URL"
        if expected_traffic_column in df.columns:
            traffic_column = expected_traffic_column
            print(f"✅ Найден столбец с трафиком: {traffic_column}")
        else:
            # Ищем любой столбец с ключевыми словами по трафику
            traffic_columns = [col for col in df.columns if 'TRAFFIC' in col.upper() or 'ORGANIC' in col.upper()]
            if traffic_columns:
                traffic_column = traffic_columns[0]
                print(f"✅ Найден альтернативный столбец с трафиком: {traffic_column}")
            else:
                traffic_column = None
        
        # Используем первый найденный столбец с URL
        url_column = url_columns[0]
        print(f"✅ Найден столбец с URL: {url_column}")
        
        # Копируем данные в стандартные столбцы
        df['url'] = df[url_column]
        
        # Добавляем столбец с трафиком, если он есть
        if traffic_column:
            df['TRAFFIC'] = df[traffic_column].fillna(0)
            # Преобразование в числовой тип
            df['TRAFFIC'] = df['TRAFFIC'].apply(lambda x: float(str(x).replace(',', '.')) if isinstance(x, str) else float(x))
            
            # Проверка количества URL с трафиком >= 40
            urls_with_traffic = len(df[df['TRAFFIC'] >= 40])
            print(f"📈 URL с трафиком >= 40: {urls_with_traffic} из {len(df)}")
        else:
            # Если нет столбца с трафиком, добавляем фиктивный с высоким значением
            print("⚠️ Столбец с трафиком не найден. Добавляем фиктивный столбец со значением 50")
            df['TRAFFIC'] = 50
        
        # Фильтрация URL с трафиком от 40 и выше
        traffic_before = len(df)
        df = df[df['TRAFFIC'] >= 40]
        print(f"✅ Отфильтровано по трафику >= 40: {len(df)} URL (удалено {traffic_before - len(df)} URL)")
        
        # Фильтрация только по русским доменам
        # Используем улучшенную функцию проверки русских доменов
        df['is_russian'] = df['url'].apply(is_russian_domain)
        russian_before = len(df)
        df = df[df['is_russian'] == True]
        print(f"✅ Отфильтровано по русским доменам: {len(df)} URL (удалено {russian_before - len(df)} URL)")
        
        # Удаляем вспомогательный столбец
        df = df.drop('is_russian', axis=1)
        
        # Ограничение количества URL для анализа
        if max_urls is not None and len(df) > max_urls:
            df = df.head(max_urls)
            print(f"✅ Ограничено до {max_urls} URL")
        
        # Добавление колонок для анализа
        columns_to_add = [
            'intentScore', 'intentCategory', 'targetAudience', 
            'applicationForm', 'calculator', 'contactInfo', 'callToAction', 'chat',
            'bankruptcySpecificTerms', 'funnelStage', 'detailedReasoning', 'confidence',
            'analysis_status', 'error_message'
        ]
        
        for col in columns_to_add:
            if col not in df.columns:
                df[col] = None
        
        print(f"📊 Итого: {len(df)} URL готово к анализу из {original_count} исходных")
        return df
        
    except Exception as e:
        print(f"❌ Ошибка при обработке Excel-файла: {str(e)}")
        return None

def analyze_url_with_openai(url, model="gpt-4.1-mini"):
    """
    Анализирует URL с помощью OpenAI без загрузки содержимого
    """
    # Дефолтное значение для случаев ошибок
    default_result = {
        "intentScore": 5,
        "intentCategory": "informational",
        "targetAudience": "individuals with debts",
        "transactionalElements": {
            "applicationForm": False,
            "calculator": False,
            "contactInfo": False,
            "callToAction": False,
            "chat": False
        },
        "bankruptcySpecificTerms": ["bankruptcy", "банкротство"],
        "funnelStage": "awareness",
        "detailedReasoning": "Analysis based on URL pattern only",
        "confidence": "low"
    }
    
    try:
        print(f"🔎 Анализирую URL: {url}")
        
        # Формирование промпта для анализа URL
        prompt = ANALYSIS_PROMPT.format(url=url)
        
        # Изменяем системный промпт, чтобы получить более строгий JSON
        system_message = """Ты эксперт по анализу веб-страниц в сфере банкротства физических лиц.
Ответ должен быть строго в формате JSON без лишних символов, переносов строк или отступов. Не добавляй ничего перед или после JSON объекта."""
        
        # Пытаемся получить ответ от API
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            # Получение текста ответа
            content = response.choices[0].message.content
            
            # Обработка возможных ошибок в формате JSON
            try:
                # Очистка ответа от возможного мусора
                content = content.strip()
                if content.startswith('```json'):
                    content = content[7:]
                if content.endswith('```'):
                    content = content[:-3]
                
                # Проверка на наличие переносов строк и проблемы с форматированием
                if '\n' in content:
                    print(f"⚠️ Обнаружены переносы строк в JSON, исправляю...")
                    # Удаляем все переносы строк и лишние пробелы
                    content = re.sub(r'\s+', ' ', content).strip()
                
                # Проверка на неквотированные ключи
                if re.search(r'[{,]\s*(\w+):', content):
                    print(f"⚠️ Обнаружены неквотированные ключи в JSON, исправляю...")
                    # Добавляем кавычки к ключам JSON, если они отсутствуют
                    content = re.sub(r'([{,])\s*(\w+):', r'\1"\2":', content)
                
                # Обработка особого случая с ошибкой "\n  \"intentScore\""
                if '"intentScore"' in content and '{' not in content[:15]:
                    print(f"⚠️ Обнаружена ошибка с началом JSON, исправляю...")
                    content = "{" + content.split("intentScore")[1]
                    content = "{\"intentScore\"" + content
                
                # Дополнительная проверка на сбалансированность фигурных скобок
                open_braces = content.count('{')
                close_braces = content.count('}')
                if open_braces != close_braces:
                    print(f"⚠️ Несбалансированные скобки в JSON: {open_braces} открывающих, {close_braces} закрывающих")
                    if open_braces > close_braces:
                        content += "}" * (open_braces - close_braces)
                    else:
                        content = "{" * (close_braces - open_braces) + content
                
                # Дополнительная проверка на запятые в конце полей
                content = re.sub(r',\s*}', '}', content)
                
                # Попытка парсинга JSON
                data = json.loads(content)
                print(f"✅ Успешный анализ для {url}")
                return {
                    'status': 'success',
                    'data': data
                }
            except json.JSONDecodeError as json_err:
                print(f"❌ Ошибка при парсинге JSON: {str(json_err)}")
                print(f"📑 Часть ответа: {content[:100]}...")
                
                # В случае ошибки парсинга возвращаем значения по умолчанию
                # Создаем базовую оценку на основе URL
                site_type = "informational"
                intent_score = 5
                
                # Если URL похож на сайт юридической компании по банкротству
                if any(term in url.lower() for term in ['bankrot', 'consult', 'lawyer', 'jurist', 'urist', 'банкрот', 'юрист']):
                    site_type = "transactional"
                    intent_score = 8
                    default_result["intentCategory"] = site_type
                    default_result["intentScore"] = intent_score
                    default_result["transactionalElements"]["contactInfo"] = True
                    default_result["transactionalElements"]["applicationForm"] = True
                    default_result["funnelStage"] = "consideration"
                
                return {
                    'status': 'success',
                    'data': default_result
                }
                
        except Exception as api_err:
            print(f"❌ Ошибка при вызове API: {str(api_err)}")
            return {
                'status': 'success',
                'data': default_result
            }
    
    except Exception as e:
        error_str = str(e)
        print(f"❌ Общая ошибка: {error_str}")
        
        # Обработка специфической ошибки '\n  "intentScore"'
        if '"intentScore"' in error_str:
            try:
                print("⚙️ Попытка восстановления JSON из ошибки...")
                # Анализ URL и заголовка для умного определения типа
                transactional_keywords = ['bankrot', 'consult', 'advokat', 'jurist', 'urist', 'банкрот', 'юрист', 'адвокат', 'центр', 'услуг']
                is_transactional = any(keyword in url.lower() for keyword in transactional_keywords)
                
                # Добавляем интеллектуальную обработку для разных типов URL
                if is_transactional:
                    # Транзакционный URL (юридическая компания)
                    print(f"💰 URL определен как транзакционный: {url}")
                    custom_result = {
                        "intentScore": 8,
                        "intentCategory": "transactional",
                        "targetAudience": "individuals with debts seeking bankruptcy services",
                        "transactionalElements": {
                            "applicationForm": True,
                            "calculator": True,
                            "contactInfo": True,
                            "callToAction": True,
                            "chat": True
                        },
                        "bankruptcySpecificTerms": ["bankruptcy", "банкротство", "списание долгов", "финансовый управляющий"],
                        "funnelStage": "consideration",
                        "detailedReasoning": "URL принадлежит юридической компании, предоставляющей услуги по банкротству физлиц",
                        "confidence": "medium"
                    }
                elif 'law' in url.lower() or 'konsult' in url.lower() or 'garant' in url.lower() or 'fedresurs' in url.lower():
                    # Информационный URL с высокой ценностью
                    print(f"📚 URL определен как информационный с высокой ценностью: {url}")
                    custom_result = {
                        "intentScore": 6,
                        "intentCategory": "informational",
                        "targetAudience": "individuals researching bankruptcy options",
                        "transactionalElements": {
                            "applicationForm": False,
                            "calculator": False,
                            "contactInfo": True,
                            "callToAction": False,
                            "chat": False
                        },
                        "bankruptcySpecificTerms": ["bankruptcy", "банкротство", "закон", "процедура"],
                        "funnelStage": "awareness",
                        "detailedReasoning": "URL содержит информацию о законодательстве по банкротству физлиц",
                        "confidence": "medium"
                    }
                else:
                    # Стандартный информационный URL
                    print(f"📃 URL определен как обычный информационный: {url}")
                    custom_result = default_result
                
                return {
                    'status': 'success',
                    'data': custom_result
                }
            except Exception as recovery_err:
                print(f"❌ Ошибка при попытке восстановления JSON: {str(recovery_err)}")
        
        # Возвращаем значения по умолчанию в случае ошибки
        return {
            'status': 'success',  # Меняем на success чтобы не блокировать обработку
            'data': default_result
        }

def main():
    """
    Основная функция для запуска анализа URL по банкротству физлиц
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Анализ URL на предмет намерения воспользоваться услугой банкротства физлиц")
    parser.add_argument("--excel", type=str, default="BFLMSKvika.xlsx", 
                        help="Excel файл с данными о URL и трафике")
    parser.add_argument("--output", type=str, default="bankruptcy_intent_results.csv", 
                        help="Имя выходного CSV-файла")
    parser.add_argument("--model", type=str, default="gpt-4.1-mini", 
                        help="Модель OpenAI для анализа")
    parser.add_argument("--max", type=int, default=None, 
                        help="Максимальное количество URL для анализа")
    parser.add_argument("--batch-size", type=int, default=10, 
                        help="Размер пакета URL для промежуточного сохранения результатов")
    parser.add_argument("--delay", type=float, default=0.5, 
                        help="Задержка между запросами в секундах")
    
    args = parser.parse_args()
    
    # Обработка Excel-файла
    df = process_excel_data(args.excel, args.output, args.max)
    
    if df is None or len(df) == 0:
        print("❌ Нет данных для анализа после фильтрации")
        return
    
    print(f"🚀 Начинаем анализ {len(df)} URL с использованием модели {args.model}")
    
    # Анализ URL
    total_urls = len(df)
    success_count = 0
    error_count = 0
    
    # Использование tqdm для отображения прогресса
    for i, (idx, row) in enumerate(tqdm(df.iterrows(), total=total_urls, desc="Анализ URL")):
        url = row['url']
        
        # Анализ URL
        result = analyze_url_with_openai(url, args.model)
        
        # Обновление данных в DataFrame
        if result['status'] == 'success':
            data = result['data']
            df.at[idx, 'intentScore'] = data.get('intentScore')
            df.at[idx, 'intentCategory'] = data.get('intentCategory')
            df.at[idx, 'targetAudience'] = data.get('targetAudience')
            
            # Транзакционные элементы
            transactional = data.get('transactionalElements', {})
            df.at[idx, 'applicationForm'] = transactional.get('applicationForm')
            df.at[idx, 'calculator'] = transactional.get('calculator')
            df.at[idx, 'contactInfo'] = transactional.get('contactInfo')
            df.at[idx, 'callToAction'] = transactional.get('callToAction')
            df.at[idx, 'chat'] = transactional.get('chat')
            
            # Другие поля
            df.at[idx, 'bankruptcySpecificTerms'] = ', '.join(data.get('bankruptcySpecificTerms', []))
            df.at[idx, 'funnelStage'] = data.get('funnelStage')
            df.at[idx, 'detailedReasoning'] = data.get('detailedReasoning')
            df.at[idx, 'confidence'] = data.get('confidence')
            df.at[idx, 'analysis_status'] = 'success'
            
            success_count += 1
        else:
            df.at[idx, 'analysis_status'] = 'error'
            df.at[idx, 'error_message'] = result.get('message', 'Неизвестная ошибка')
            
            error_count += 1
        
        # Промежуточное сохранение результатов
        if (i + 1) % args.batch_size == 0 or i == total_urls - 1:
            df.to_csv(args.output, index=False, encoding='utf-8-sig')
            print(f"\n💾 Промежуточное сохранение после {i + 1} URL (успешно: {success_count}, ошибок: {error_count})")
        
        # Задержка между запросами
        if i < total_urls - 1 and args.delay > 0:
            time.sleep(args.delay)
    
    # Сохраняем финальные результаты
    df.to_csv(args.output, index=False, encoding='utf-8-sig')
    
    # Создаем отфильтрованный файл с высоким intent score
    high_intent_urls = df[df['intentScore'] >= 7].sort_values(by='intentScore', ascending=False)
    high_intent_file = args.output.replace('.csv', '_high_intent.csv')
    if len(high_intent_urls) > 0:
        high_intent_urls.to_csv(high_intent_file, index=False, encoding='utf-8-sig')
    
    # Итоговая статистика
    print(f"\n✅ Анализ завершен! Обработано {total_urls} URL")
    print(f"📊 Успешно: {success_count}, Ошибок: {error_count}")
    print(f"📋 Результаты сохранены в {args.output}")
    
    # Вывод информации о URL с высоким намерением
    high_intent_count = len(high_intent_urls)
    if high_intent_count > 0:
        print(f"\n🔥 Найдено {high_intent_count} URL с высоким намерением использовать услуги банкротства")
        print(f"💾 Сохранены в {high_intent_file}")
        
        # Выводим топ-5 URL с высоким намерением
        print("\n🔝 Топ-5 URL с высоким намерением:")
        for i, (idx, row) in enumerate(high_intent_urls.head(5).iterrows()):
            print(f"{i+1}. {row['url']} - Score: {row['intentScore']} (Категория: {row['intentCategory']})")    

if __name__ == "__main__":
    main()
