import argparse
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


def extract_push_content(messages: list) -> str:
	"""從 messages 中提取推文內容"""
	if not messages:
		return ""
	
	push_lines = []
	for msg in messages:
		push_content = msg.get("push_content", "")
		if push_content:
			push_lines.append(f"推文: {push_content}")
	
	return "\n".join(push_lines)


def clean_article_with_comments(article: dict) -> str:
	"""清洗文章內容，只包含主要文字內容（用於向量檢索）"""
	# 提取主文
	content = article.get("content", "") if isinstance(article, dict) else str(article)

	# 提取推文內容
	messages = article.get("messages", [])
	push_content = extract_push_content(messages)
	
	# 組合主要文字內容：主文 + 推文
	parts = []

	if content:
		parts.append(content)
	
	if push_content:
		parts.append(push_content)
	
	return " | ".join(parts) if parts else ""


def normalize_datetime(dt_value) -> str:
	"""將常見的日期時間格式正規化為 ISO 8601。未知時回傳空字串。"""
	if not dt_value:
		return ""
	if isinstance(dt_value, (int, float)):
		try:
			return datetime.fromtimestamp(float(dt_value)).isoformat()
		except Exception:
			return ""
	if isinstance(dt_value, str):
		candidates = [
			"%Y/%m/%d %H:%M",
			"%Y-%m-%d %H:%M",
			"%Y/%m/%d",
			"%Y-%m-%d",
			"%a %b %d %H:%M:%S %Y",
		]
		for fmt in candidates:
			try:
				return datetime.strptime(dt_value, fmt).isoformat()
			except Exception:
				pass
		# 直接回傳原字串，避免資訊流失
		return dt_value
	return ""


def extract_title_tags(title: str) -> List[str]:
	if not title:
		return []
	# 取出如 [情報][閒聊] 之類的標籤
	tags = re.findall(r"\[(.+?)\]", title)
	return [t.strip() for t in tags if t.strip()]


def simple_chunk_text(text: str, max_chars: int = 1600, overlap: int = 200) -> List[str]:
	"""以字元數切塊，帶重疊。避免外部依賴，適合中文語料。"""
	text = text.strip()
	if not text:
		return []
	chunks: List[str] = []
	start = 0
	length = len(text)
	if max_chars <= 0:
		return [text]
	while start < length:
		end = min(start + max_chars, length)
		chunks.append(text[start:end])
		if end == length:
			break
		start = max(0, end - overlap)
	return chunks

def chunk_with_title_prefix(text: str, title: str, max_chars: int = 1600, overlap: int = 200) -> List[str]:
	"""切塊時為每個 chunk 加入標題前綴"""
	chunks = simple_chunk_text(text, max_chars, overlap)
	
	if not title:
		return chunks
	
	# 為每個 chunk 加入標題前綴
	title_prefix = f"標題: {title} | "
	title_prefix_length = len(title_prefix)
	
	# 調整 max_chars 以容納標題前綴
	adjusted_max_chars = max_chars - title_prefix_length
	
	# 重新切塊
	adjusted_chunks = simple_chunk_text(text, adjusted_max_chars, overlap)
	
	# 為每個 chunk 加入標題前綴
	result = []
	for chunk in adjusted_chunks:
		result.append(f"{title_prefix}{chunk}")
	
	return result


def sha256_hexdigest(content: str) -> str:
	return hashlib.sha256(content.encode("utf-8", errors="ignore")).hexdigest()


def to_jsonl_records(article: Dict, source: str = "PTT") -> Iterable[Dict]:
	# 防禦式取值：不同資料來源欄位名稱可能不同
	article_id = (
		article.get("id") or
		article.get("aid") or
		article.get("article_id") or
		article.get("_id") or
		""
	)
	url = article.get("url", "")
	board = article.get("board", article.get("board_name", ""))
	author = article.get("author", "")
	created_at = normalize_datetime(
		article.get("date") or article.get("created_at") or article.get("time")
	)
	
	# 提取主要文字內容（用於向量檢索）
	content = clean_article_with_comments(article)

	# 提取標題和標籤
	article_title = article.get("article_title", "")
	tags = extract_title_tags(article_title)

	# # 推文統計資訊
	# message_count = article.get("message_count", {})
	# push_count = message_count.get("push", 0)
	# boo_count = message_count.get("boo", 0)
	# total_responses = message_count.get("all", 0)
	
	# # 計算熱度指標
	# heat_level = "低熱度"
	# if total_responses > 50:
	# 	heat_level = "高熱度"
	# elif total_responses > 20:
	# 	heat_level = "中熱度"
	
	# # 計算情感傾向
	# sentiment = "中性"
	# if push_count > boo_count:
	# 	sentiment = "正面"
	# elif boo_count > push_count:
	# 	sentiment = "負面"
	
	# 提取 IP 位址
	# ip = article.get("ip", "")
	
	# 提取發文時間
	date = article.get("date", "")

	# 去重 key（依完整內容）
	dedupe_key = f"sha256:{sha256_hexdigest(content)}"

	# 切塊（為每個 chunk 加入標題前綴）
	content_chunks = chunk_with_title_prefix(content, article_title)
	total_chunks = len(content_chunks) if content_chunks else 1

	# 若沒有內容，仍輸出一個空塊，方便後續去重與對齊
	if not content_chunks:
		content_chunks = [""]

	for idx, chunk in enumerate(content_chunks):
		
		record = {
			"content": chunk,
			"metadata": {
				"id": f"{source}_{board}_{article_id}_{idx}".strip("_"),
				"source": source,
				"url": url,
				"board": board,
				"title": article_title,
				"author": author,
				"created_at": created_at,
				"tags": tags,
				"chunk_index": idx,
				"total_chunks": total_chunks,
				"has_media": bool(article.get("has_media", False)),
				"dedupe_key": dedupe_key,
				# 推薦相關欄位
				# "push_count": push_count,
				# "boo_count": boo_count,
				# "total_responses": total_responses,
				# "heat_level": heat_level,
				# "sentiment": sentiment,
				# "ip": ip,
				# "date": date,
			}
		}
		yield record


def convert_to_jsonl(input_path: Path, output_path: Path, source: str = "PTT") -> Tuple[int, int]:
	"""回傳 (總文章數, 輸出行數)。"""
	with input_path.open("r", encoding="utf-8") as f:
		data = json.load(f)

	count_articles = 0
	count_lines = 0
	with output_path.open("w", encoding="utf-8") as out:
		for article in data["articles"]:
			title = article.get("article_title", "")
			if "[原創]" in title:
				continue 
			
			count_articles += 1
			for rec in to_jsonl_records(article, source=source):
				out.write(json.dumps(rec, ensure_ascii=False) + "\n")
				count_lines += 1

	return count_articles, count_lines


def main() -> None:

	input_path = Path("CFantasy-2-4000.json")
	if not input_path.exists():
		raise FileNotFoundError(f"Input not found: {input_path}")

	output_path = Path("CFantasy-2-4000_cleaned.jsonl")
	articles, lines = convert_to_jsonl(input_path, output_path, source="PTT")
	print(f"Converted articles: {articles}")
	print(f"Output JSONL lines (chunks): {lines}")
	print(f"Saved to: {output_path}")


if __name__ == "__main__":
	main()


