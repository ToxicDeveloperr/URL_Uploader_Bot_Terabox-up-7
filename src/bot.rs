use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use async_read_progress::TokioAsyncReadProgressExt;
use dashmap::{DashMap, DashSet};
use futures::TryStreamExt;
use grammers_client::{
    button, reply_markup,
    types::{CallbackQuery, Chat, Message, User},
    Client, InputMessage, Update,
};
use log::{error, info, warn};
use reqwest::Url;
use scopeguard::defer;
use serde_json::Value;
use stream_cancel::{Trigger, Valved};
use tokio::sync::{mpsc, Mutex};
use tokio_util::compat::FuturesAsyncReadCompatExt;

use crate::command::{parse_command, Command};

/// Bot is the main struct of the bot.
/// All the bot logic is implemented in this struct.
#[derive(Debug)]
pub struct Bot {
    client: Client,
    me: User,
    http: reqwest::Client,
    locks: Arc<DashSet<i64>>,
    started_by: Arc<DashMap<i64, i64>>,
    triggers: Arc<DashMap<i64, Trigger>>,

    // Cache known chats we have seen in updates (id -> Chat)
    known_chats: Arc<DashMap<i64, Chat>>,

    // Queue for source-channel links
    queue_tx: mpsc::Sender<String>,

    // Configured source/destination (by id)
    source_channel_id: Option<i64>,
    destination_channel_id: Option<i64>,

    // Optional destination username for resolution (e.g., @mychannel without @)
    destination_channel_username: Option<String>,
}

// Very simple URL extractor: splits by whitespace and keeps http(s) tokens,
// trimming common surrounding punctuation.
fn extract_urls(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    for raw in text.split_whitespace() {
        let token = raw.trim_matches(|c: char| matches!(
            c,
            ',' | ';' | '.' | ')' | '(' | ']' | '[' | '>' | '<' | '"' | '\''
        ));
        if token.starts_with("http://") || token.starts_with("https://") {
            out.push(token);
        }
    }
    out
}

/// Convert Bot API-style channel/supergroup id (-100xxxxxxxxxxxx) to MTProto internal id (xxxxxxxxxxxx)
fn normalize_chat_id(id: i64) -> i64 {
    // If it's like -1001234567890, convert to 1234567890
    if id <= -1_000_000_000_000 {
        (id.abs() - 1_000_000_000_000) as i64
    } else {
        id
    }
}

impl Bot {
    /// Create a new bot instance.
pub async fn new(client: Client) -> Result<Arc<Self>> {
        let me = client.get_me().await?;

        // Read optional channel ids from env and normalize if they are in Bot API format (-100...)
        let source_channel_id = std::env::var("SOURCE_CHANNEL_ID")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .map(normalize_chat_id);
        let destination_channel_id = std::env::var("DESTINATION_CHANNEL_ID")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .map(normalize_chat_id);

        let (queue_tx, queue_rx) = mpsc::channel::<String>(200);

        let dest_username = std::env::var("DESTINATION_CHANNEL_USERNAME").ok().map(|s| s.trim_start_matches('@').to_string());

        let bot = Arc::new(Self {
            client,
            me,
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
                .build()?,
            locks: Arc::new(DashSet::new()),
            started_by: Arc::new(DashMap::new()),
            triggers: Arc::new(DashMap::new()),
            known_chats: Arc::new(DashMap::new()),
            queue_tx,
            source_channel_id,
            destination_channel_id,
            destination_channel_username: dest_username,
        });

        // Spawn queue worker if both ids are configured
        if bot.source_channel_id.is_some() && bot.destination_channel_id.is_some() {
            info!(
                "Normalized channel ids -> source: {:?}, destination: {:?}",
                bot.source_channel_id, bot.destination_channel_id
            );
            let bot_clone = bot.clone();
            tokio::spawn(async move {
                if let Err(e) = bot_clone.queue_worker(queue_rx).await {
                    error!("Queue worker error: {}", e);
                }
            });
            info!(
                "Queue worker started (source: {:?}, destination: {:?})",
                bot.source_channel_id, bot.destination_channel_id
            );
        } else {
            info!(
                "Queue worker disabled. Configure SOURCE_CHANNEL_ID and DESTINATION_CHANNEL_ID to enable."
            );
        }

        Ok(bot)
    }

    /// Check if URL is from Terabox domain
    fn is_terabox_url(&self, url: &Url) -> bool {
        if let Some(domain) = url.domain() {
            let domain = domain.to_lowercase();
            domain.contains("terabox") || 
            domain.contains("1024tera") || 
            domain.contains("4funbox") ||
            domain.contains("mirrobox") ||
            domain.contains("nephobox") ||
            domain.contains("terasharelink") ||
            domain.contains("terafileshare")
        } else {
            false
        }
    }

    /// Get final stream link for Terabox URLs
    async fn get_final_stream_link(&self, terabox_url: &str) -> Result<Option<String>> {
        let api_url = "https://vercel-api-2-ecru.vercel.app/fetch";
    
        let response = self.http
            .get(api_url)
            .query(&[("url", terabox_url)])
            .send()
            .await?;
    
        if response.status().is_success() {
            let text = response.text().await?;
            // Debug ke liye
            println!("Raw API response: {}", text);
    
            // JSON parse karne ki try
            if let Ok(data) = serde_json::from_str::<Value>(&text) {
                if let Some(proxy_url) = data.get("proxy_url") {
                    if let Some(url_str) = proxy_url.as_str() {
                        return Ok(Some(url_str.to_string()));
                    }
                }
            } else {
                eprintln!("Failed to parse JSON from API response");
            }
        }
    
        Ok(None)
    }


    /// Run the bot.
    pub async fn run(self: Arc<Self>) {
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Received Ctrl+C, exiting");
                    break;
                }

                Ok(update) = self.client.next_update() => {
                    let self_ = self.clone();

                    // Spawn a new task to handle the update
                    tokio::spawn(async move {
                        if let Err(err) = self_.handle_update(update).await {
                            error!("Error handling update: {}", err);
                        }
                    });
                }
            }
        }
    }

    /// Update handler.
    async fn handle_update(&self, update: Update) -> Result<()> {
        // NOTE: no ; here, so result is returned
        match update {
            Update::NewMessage(msg) => self.handle_message(msg).await,
            Update::CallbackQuery(query) => self.handle_callback(query).await,
            _ => Ok(()),
        }
    }

    /// Message handler.
    ///
    /// Ensures the message is from a user or a group, and then parses the command.
    /// If the command is not recognized, it will try to parse the message as a URL.
async fn handle_message(&self, msg: Message) -> Result<()> {
        // Basic visibility: log every new message with chat kind
        // Cache the chat so we can send messages later without dialogs
        self.known_chats.insert(msg.chat().id(), msg.chat().clone());

        let kind = match msg.chat() {
            Chat::User(_) => "user",
            Chat::Group(_) => "group",
            _ => "other",
        };
        info!("New message in chat {} (kind: {})", msg.chat().id(), kind);

        // Special handling: if a source channel is configured and this message is from that channel,
        // extract links and enqueue them for processing to destination channel.
        if let (Some(source_id), Some(_dest_id)) = (self.source_channel_id, self.destination_channel_id) {
            if msg.chat().id() == source_id {
                let text = msg.text();
                let urls = extract_urls(text);
                if urls.is_empty() {
                    info!("No URLs found in source channel message");
                    return Ok(());
                }
                info!("Enqueuing {} link(s) from source channel {}", urls.len(), source_id);
                for url in urls {
                    // Enqueue with backpressure so nothing is dropped
                    if let Err(e) = self.queue_tx.send(url.to_string()).await {
                        warn!("Queue closed, failed to enqueue url: {}", e);
                    }
                }
                return Ok(());
            } else if !matches!(msg.chat(), Chat::User(_) | Chat::Group(_)) {
                // If it's a channel/similar but ID doesn't match, log the observed ID for debug
                info!(
                    "Ignoring non-source channel message. Observed chat id: {} (configure this as SOURCE_CHANNEL_ID if this is your source)",
                    msg.chat().id()
                );
            }
        }

        // Ensure the message chat is a user or a group
        match msg.chat() {
            Chat::User(_) | Chat::Group(_) => {}
            _ => return Ok(()),
        };

        // Parse the command
        let command = parse_command(msg.text());
        if let Some(command) = command {
            // Ensure the command is for this bot
            if let Some(via) = &command.via {
                if via.to_lowercase() != self.me.username().unwrap_or_default().to_lowercase() {
                    warn!("Ignoring command for unknown bot: {}", via);
                    return Ok(());
                }
            }

            // There is a chance that there are multiple bots listening
            // to /start commands in a group, so we handle commands
            // only if they are sent explicitly to this bot.
            if let Chat::Group(_) = msg.chat() {
                if command.name == "start" && command.via.is_none() {
                    return Ok(());
                }
            }

            // Handle the command
            info!("Received command: {:?}", command);
            match command.name.as_str() {
                "start" => {
                    return self.handle_start(msg).await;
                }
                "upload" => {
                    return self.handle_upload(msg, command).await;
                }
                _ => {}
            }
        }

        if let Chat::User(_) = msg.chat() {
            // If the message is not a command, try to parse it as a URL
            if let Ok(url) = Url::parse(msg.text()) {
                return self.handle_url(msg, url).await;
            }
        }

        Ok(())
    }

    /// Handle the /start command.
    /// This command is sent when the user starts a conversation with the bot.
    /// It will reply with a welcome message.
    async fn handle_start(&self, msg: Message) -> Result<()> {
        msg.reply(InputMessage::html(
            "📁 <b>Hi! Need a file uploaded? Just send the link!</b>\n\
            In groups, use <code>/upload &lt;url&gt;</code>\n\
            \n\
            🌟 <b>Features:</b>\n\
            \u{2022} Free & fast\n\
            \u{2022} <a href=\"https://github.com/HerMan-Official/URL_Uploader_Bot_Telegram\">Open source</a>\n\
            \u{2022} Uploads files up to 2GB\n\
            \u{2022} Redirect-friendly\n\
            \u{2022} <b>Terabox support</b>",
        ))
        .await?;
        Ok(())
    }

    /// Handle the /upload command.
    /// This command should be used in groups to upload a file.
    async fn handle_upload(&self, msg: Message, cmd: Command) -> Result<()> {
        // If the argument is not specified, reply with an error
        let url = match cmd.arg {
            Some(url) => url,
            None => {
                msg.reply("Please specify a URL").await?;
                return Ok(());
            }
        };

        // Parse the URL
        let url = match Url::parse(&url) {
            Ok(url) => url,
            Err(err) => {
                msg.reply(format!("Invalid URL: {}", err)).await?;
                return Ok(());
            }
        };

        self.handle_url(msg, url).await
    }

/// Handle a URL.
    /// This function will download the file and upload it to the same chat as msg.
    async fn handle_url(&self, msg: Message, url: Url) -> Result<()> {
        let sender = match msg.sender() {
            Some(sender) => sender,
            None => return Ok(()),
        };

        // Lock the chat to prevent multiple uploads at the same time
        info!("Locking chat {}", msg.chat().id());
        let _lock = self.locks.insert(msg.chat().id());
        if !_lock {
            msg.reply("✋ Whoa, slow down! There's already an active upload in this chat.")
                .await?;
            return Ok(());
        }
        self.started_by.insert(msg.chat().id(), sender.id());

        // Deferred unlock
        defer! {
            info!("Unlocking chat {}", msg.chat().id());
            self.locks.remove(&msg.chat().id());
            self.started_by.remove(&msg.chat().id());
        };

        // Check if it's a Terabox URL and get the proxy URL if needed
        let download_url = if self.is_terabox_url(&url) {
            info!("Detected Terabox URL: {}", url);
            msg.reply("🔄 Processing Terabox link...").await?;
            
            match self.get_final_stream_link(url.as_str()).await {
                Ok(Some(proxy_url)) => {
                    info!("Got proxy URL for Terabox: {}", proxy_url);
                    match Url::parse(&proxy_url) {
                        Ok(parsed_url) => parsed_url,
                        Err(err) => {
                            msg.reply(format!("❌ Failed to parse proxy URL: {}", err)).await?;
                            return Ok(());
                        }
                    }
                }
                Ok(None) => {
                    msg.reply("❌ Failed to get download link from Terabox. The file might be private or the link is invalid.").await?;
                    return Ok(());
                }
                Err(err) => {
                    error!("Error getting Terabox proxy URL: {}", err);
                    msg.reply("❌ Failed to process Terabox link. Please try again.").await?;
                    return Ok(());
                }
            }
        } else {
            // Use the original URL for non-Terabox links
            url
        };

        info!("Downloading file from {}", download_url);
        let response = self.http.get(download_url).send().await?;

        // Get the file name and size
        let length = response.content_length().unwrap_or_default() as usize;
        let name = match response
            .headers()
            .get("content-disposition")
            .and_then(|value| {
                value
                    .to_str()
                    .ok()
                    .and_then(|value| {
                        value
                            .split(';')
                            .map(|value| value.trim())
                            .find(|value| value.starts_with("filename="))
                    })
                    .map(|value| value.trim_start_matches("filename="))
                    .map(|value| value.trim_matches('"'))
            }) {
            Some(name) => name.to_string(),
            None => response
                .url()
                .path_segments()
                .and_then(|segments| segments.last())
                .and_then(|name| {
                    if name.contains('.') {
                        Some(name.to_string())
                    } else {
                        // guess the extension from the content type
                        response
                            .headers()
                            .get("content-type")
                            .and_then(|value| value.to_str().ok())
                            .and_then(mime_guess::get_mime_extensions_str)
                            .and_then(|ext| ext.first())
                            .map(|ext| format!("{}.{}", name, ext))
                    }
                })
                .unwrap_or("file.bin".to_string())
                .to_string(),
        };
        let name = percent_encoding::percent_decode_str(&name)
            .decode_utf8()?
            .to_string();
        let lower_name = name.to_lowercase();
        
        // Check content type and file extension for video files
        let content_type_is_video = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(|value| {
                value.starts_with("video/") || 
                value.contains("x-matroska") ||  // For MKV files
                value.contains("quicktime")    // For MOV files
            })
            .unwrap_or(false);
            
        let video_extensions = [
            ".mp4", ".mkv", ".webm", ".avi", ".mov", 
            ".m4v", ".3gp", ".flv", ".wmv", ".ts"
        ];
        
        let is_video = content_type_is_video || 
            video_extensions.iter().any(|ext| lower_name.ends_with(ext));
            
        info!("File {} ({} bytes, video: {})", name, length, is_video);

        // File is empty
        if length == 0 {
            msg.reply("⚠️ File is empty").await?;
            return Ok(());
        }

        // File is too large
        if length > 2 * 1024 * 1024 * 1024 {
            msg.reply("⚠️ File is too large").await?;
            return Ok(());
        }

        // Wrap the response stream in a valved stream
        let (trigger, stream) = Valved::new(
            response
                .bytes_stream()
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err)),
        );
        self.triggers.insert(msg.chat().id(), trigger);

        // Deferred trigger removal
        defer! {
            self.triggers.remove(&msg.chat().id());
        };

        // Reply markup buttons
        let reply_markup = Arc::new(reply_markup::inline(vec![vec![button::inline(
            "⛔ Cancel",
            "cancel",
        )]]));

        // Send status message
        let status = Arc::new(Mutex::new(
            msg.reply(
                InputMessage::html(format!("🚀 Starting upload of <code>{}</code>...", name))
                    .reply_markup(reply_markup.clone().as_ref()),
            )
            .await?,
        ));

        let mut stream = stream
            .into_async_read()
            .compat()
            // Report progress every 3 seconds
            .report_progress(Duration::from_secs(3), |progress| {
                let status = status.clone();
                let name = name.clone();
                let reply_markup = reply_markup.clone();
                tokio::spawn(async move {
                    status
                        .lock()
                        .await
                        .edit(
                            InputMessage::html(format!(
                                "⏳ Uploading <code>{}</code> <b>({:.2}%)</b>\n\
                            <i>{} / {}</i>",
                                name,
                                progress as f64 / length as f64 * 100.0,
                                bytesize::to_string(progress as u64, true),
                                bytesize::to_string(length as u64, true),
                            ))
                            .reply_markup(reply_markup.as_ref()),
                        )
                        .await
                        .ok();
                });
            });

        // Upload the file
        let start_time = chrono::Utc::now();
        let file = self
            .client
            .upload_stream(&mut stream, length, name.clone())
            .await?;

        // Calculate upload time
        let elapsed = chrono::Utc::now() - start_time;
        info!("Uploaded file {} ({} bytes) in {}", name, length, elapsed);

        // Send file
        let caption = format!(
            "Uploaded in <b>{:.2} secs</b>",
            elapsed.num_milliseconds() as f64 / 1000.0
        );

        let mut input_msg = InputMessage::html(caption);
        if is_video {
            input_msg = input_msg.document(file).attribute(grammers_client::types::Attribute::Video {
                supports_streaming: true,
                duration: Duration::ZERO,
                w: 0,
                h: 0,
                round_message: false,
            });
        } else {
            input_msg = input_msg.document(file);
        };
        msg.reply(input_msg).await?;

        // Delete status message
        status.lock().await.delete().await?;

        Ok(())
    }

    /// Callback query handler.
    async fn handle_callback(&self, query: CallbackQuery) -> Result<()> {
        match query.data() {
            b"cancel" => self.handle_cancel(query).await,
            _ => Ok(()),
        }
    }

    /// Handle the cancel button.
    async fn handle_cancel(&self, query: CallbackQuery) -> Result<()> {
        // If the upload was initiated from channel-queue, there is no started_by entry.
        // So we allow cancellation only for interactive (DM/group) uploads initiated by a user.
        let started_by_user_id = match self.started_by.get(&query.chat().id()) {
            Some(id) => *id,
            None => return Ok(()),
        };

        if started_by_user_id != query.sender().id() {
            info!(
                "Some genius with ID {} tried to cancel another user's upload in chat {}",
                query.sender().id(),
                query.chat().id()
            );

            query
                .answer()
                .alert("⚠️ You can't cancel another user's upload")
                .cache_time(Duration::ZERO)
                .send()
                .await?;

            return Ok(());
        }

        if let Some((chat_id, trigger)) = self.triggers.remove(&query.chat().id()) {
            info!("Cancelling upload in chat {}", chat_id);
            drop(trigger);
            self.started_by.remove(&chat_id);

            query
                .load_message()
                .await?
                .edit("⛔ Upload cancelled")
                .await?;

            query.answer().send().await?;
        }
        Ok(())
    }

    // Worker: process queued URLs and upload one-by-one to the destination channel
    async fn queue_worker(self: Arc<Self>, mut rx: mpsc::Receiver<String>) -> Result<()> {
        // Keep a cached destination chat once resolved
        let mut cached_dest: Option<Chat> = None;

        loop {
            let Some(url_str) = rx.recv().await else { break; };

            let dest_id = match self.destination_channel_id {
                Some(id) => id,
                None => {
                    warn!("Destination channel not configured, dropping queued item");
                    continue;
                }
            };

            // Ensure dest chat is resolved; retry until available so we don't lose queued items
            if cached_dest.as_ref().map(|c| c.id()) != Some(dest_id) {
                cached_dest = None;
            }
            let dest_chat = match &cached_dest {
                Some(c) => c.clone(),
                None => {
                    loop {
                        match self.get_or_wait_chat(dest_id).await {
                            Ok(c) => {
                                info!("Destination chat resolved: {}", dest_id);
                                cached_dest = Some(c.clone());
                                break c;
                            }
                            Err(e) => {
                                error!(
                                    "Destination chat id {} not resolvable: {}. Ensure the bot is a member/admin. Retrying in 10s...",
                                    dest_id, e
                                );
                                tokio::time::sleep(Duration::from_secs(10)).await;
                            }
                        }
                    }
                }
            };

            let url = match Url::parse(&url_str) {
                Ok(u) => u,
                Err(e) => {
                    warn!("Invalid URL in queue: {} ({})", url_str, e);
                    continue;
                }
            };

            if let Err(e) = self.upload_url_to_chat(url, &dest_chat).await {
                error!("Upload failed for {}: {}", url_str, e);
            }
        }
        Ok(())
    }

    // Try to get a chat by id from cache or by resolving username, without using dialogs (bots can't use getDialogs)
    async fn get_or_wait_chat(&self, id: i64) -> Result<Chat> {
        // First, check cache from updates
        if let Some(chat) = self.known_chats.get(&id) {
            return Ok(chat.clone());
        }

        // Second, try resolving by username if provided
        if let Some(username) = &self.destination_channel_username {
            match self.client.resolve_username(username).await? {
                Some(chat) => {
                    // Cache and return
                    self.known_chats.insert(chat.id(), chat.clone());
                    if chat.id() != id {
                        warn!(
                            "Resolved @{} to chat id {}, but DESTINATION_CHANNEL_ID is {}. Using resolved chat.",
                            username, chat.id(), id
                        );
                    }
                    return Ok(chat);
                }
                None => {
                    warn!("Could not resolve username @{} yet", username);
                }
            }
        }

        // Finally, wait until an update for this chat arrives (bot must be member/admin)
        // We poll the cache for up to 5 minutes with 5s interval
        let mut waited = 0u64;
        while waited < 300 {
            if let Some(chat) = self.known_chats.get(&id) {
                return Ok(chat.clone());
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
            waited += 5;
        }

        anyhow::bail!("Chat id {} not available yet. Ensure the bot is a member and has posted/seen at least one event.", id)
    }

    // Core upload logic reused by queue worker (progress messages are posted in dest chat)
    async fn upload_url_to_chat(&self, url: Url, dest_chat: &Chat) -> Result<()> {
        // Check if it's a Terabox URL and get the proxy URL if needed
        let download_url = if self.is_terabox_url(&url) {
            info!("(Queue) Detected Terabox URL: {}", url);
            match self.get_final_stream_link(url.as_str()).await {
                Ok(Some(proxy_url)) => {
                    info!("(Queue) Got proxy URL for Terabox: {}", proxy_url);
                    Url::parse(&proxy_url).context("Failed to parse proxy URL")?
                }
                Ok(None) => {
                    anyhow::bail!("Failed to get download link from Terabox (None)");
                }
                Err(err) => {
                    return Err(err);
                }
            }
        } else {
            url
        };

        info!("(Queue) Downloading file from {}", download_url);
        let response = self.http.get(download_url).send().await?;

        // Get the file name and size
        let length = response.content_length().unwrap_or_default() as usize;
        let name = match response
            .headers()
            .get("content-disposition")
            .and_then(|value| {
                value
                    .to_str()
                    .ok()
                    .and_then(|value| {
                        value
                            .split(';')
                            .map(|value| value.trim())
                            .find(|value| value.starts_with("filename="))
                    })
                    .map(|value| value.trim_start_matches("filename="))
                    .map(|value| value.trim_matches('"'))
            }) {
            Some(name) => name.to_string(),
            None => response
                .url()
                .path_segments()
                .and_then(|segments| segments.last())
                .and_then(|name| {
                    if name.contains('.') {
                        Some(name.to_string())
                    } else {
                        // guess the extension from the content type
                        response
                            .headers()
                            .get("content-type")
                            .and_then(|value| value.to_str().ok())
                            .and_then(mime_guess::get_mime_extensions_str)
                            .and_then(|ext| ext.first())
                            .map(|ext| format!("{}.{}", name, ext))
                    }
                })
                .unwrap_or("file.bin".to_string())
                .to_string(),
        };
        let name = percent_encoding::percent_decode_str(&name)
            .decode_utf8()?
            .to_string();
        let lower_name = name.to_lowercase();

        // Check content type and file extension for video files
        let content_type_is_video = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(|value| {
                value.starts_with("video/") ||
                value.contains("x-matroska") ||  // For MKV files
                value.contains("quicktime")    // For MOV files
            })
            .unwrap_or(false);

        let video_extensions = [
            ".mp4", ".mkv", ".webm", ".avi", ".mov",
            ".m4v", ".3gp", ".flv", ".wmv", ".ts"
        ];

        let is_video = content_type_is_video ||
            video_extensions.iter().any(|ext| lower_name.ends_with(ext));

        info!("(Queue) File {} ({} bytes, video: {})", name, length, is_video);

        // File is empty
        if length == 0 {
            // Post a quick note to destination that the URL was empty
            let _ = self
                .client
                .send_message(dest_chat, InputMessage::html("⚠️ File is empty"))
                .await;
            return Ok(());
        }

        // File is too large
        if length > 2 * 1024 * 1024 * 1024 {
            let _ = self
                .client
                .send_message(dest_chat, InputMessage::html("⚠️ File is too large"))
                .await;
            return Ok(());
        }

        // Wrap the response stream in a valved stream (no cancel button in queue mode)
        let (_trigger, stream) = Valved::new(
            response
                .bytes_stream()
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err)),
        );

        // Send status message in destination
        let status = Arc::new(Mutex::new(
            self
                .client
                .send_message(
                    dest_chat,
                    InputMessage::html(format!("🚀 Starting upload of <code>{}</code>...", name)),
                )
                .await?,
        ));

        // Setup progress reporter
        let mut stream = stream
            .into_async_read()
            .compat()
            // Report progress every 3 seconds
            .report_progress(Duration::from_secs(3), |progress| {
                let status = status.clone();
                let name = name.clone();
                tokio::spawn(async move {
                    status
                        .lock()
                        .await
                        .edit(InputMessage::html(format!(
                            "⏳ Uploading <code>{}</code> <b>({:.2}%)</b>\n\
                            <i>{} / {}</i>",
                            name,
                            progress as f64 / length as f64 * 100.0,
                            bytesize::to_string(progress as u64, true),
                            bytesize::to_string(length as u64, true),
                        )))
                        .await
                        .ok();
                });
            });

        // Upload the file
        let start_time = chrono::Utc::now();
        let file = self
            .client
            .upload_stream(&mut stream, length, name.clone())
            .await?;

        // Calculate upload time
        let elapsed = chrono::Utc::now() - start_time;
        info!("(Queue) Uploaded file {} ({} bytes) in {}", name, length, elapsed);

        // Send file to destination
        let caption = format!(
            "Uploaded in <b>{:.2} secs</b>",
            elapsed.num_milliseconds() as f64 / 1000.0
        );

        let mut input_msg = InputMessage::html(caption);
        if is_video {
            input_msg = input_msg.document(file).attribute(grammers_client::types::Attribute::Video {
                supports_streaming: true,
                duration: Duration::ZERO,
                w: 0,
                h: 0,
                round_message: false,
            });
        } else {
            input_msg = input_msg.document(file);
        };
        self.client.send_message(dest_chat, input_msg).await?;

        // Delete status message
        status.lock().await.delete().await?;

        Ok(())
    }
}
