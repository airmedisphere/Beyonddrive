"""
Enhanced ByteStreamer for Telegram file streaming.
Fixed for Pyrogram version compatibility (KurimuzonAkuma fork).
Optimized for YouTube-like video streaming with proper chunking.
"""

import asyncio
from typing import Dict, Union
from pyrogram import Client, utils, raw
from .file_properties import get_file_ids
from pyrogram.session import Session, Auth
from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from utils.logger import Logger

logger = Logger(__name__)

# Telegram DC addresses for auth key creation
DC_ADDRESSES = {
    1: ("149.154.175.53", 443),
    2: ("149.154.167.51", 443),
    3: ("149.154.175.100", 443),
    4: ("149.154.167.91", 443),
    5: ("91.108.56.130", 443),
}

DC_ADDRESSES_TEST = {
    1: ("149.154.175.10", 443),
    2: ("149.154.167.40", 443),
    3: ("149.154.175.117", 443),
}


class ByteStreamer:
    """
    Handles streaming of files from Telegram with optimized chunking.
    Supports range requests for video seeking.
    """
    
    def __init__(self, client: Client):
        self.clean_timer = 30 * 60  # 30 minutes cache cleanup
        self.client: Client = client
        self.cached_file_ids: Dict[int, FileId] = {}
        asyncio.create_task(self.clean_cache())

    async def get_file_properties(self, channel, message_id: int) -> FileId:
        """Get file properties, using cache if available."""
        if message_id not in self.cached_file_ids:
            await self.generate_file_properties(channel, message_id)
        return self.cached_file_ids[message_id]

    async def generate_file_properties(self, channel, message_id: int) -> FileId:
        """Generate and cache file properties from Telegram."""
        file_id = await get_file_ids(self.client, channel, message_id)
        if not file_id:
            raise Exception("FileNotFound")
        self.cached_file_ids[message_id] = file_id
        return self.cached_file_ids[message_id]

    async def _create_session_with_fallback(
        self,
        client: Client,
        dc_id: int,
        auth_key: bytes,
        test_mode: bool,
        context: str = ""
    ) -> Session:
        """
        Create a Session object with robust fallback handling.

        Args:
            client: Pyrogram client instance
            dc_id: Data center ID
            auth_key: Authorization key bytes
            test_mode: Whether in test mode
            context: Context string for logging (e.g., "different DC" or "same DC")

        Returns:
            Initialized Session object

        Raises:
            Exception: If all session creation attempts fail
        """
        logger.info(f"Creating session for DC {dc_id} ({context}): test_mode={test_mode}, auth_key_len={len(auth_key) if auth_key else 0}")
        logger.debug(f"Session class: {Session}, Module: {Session.__module__}")

        # Attempt 1: Using explicit KEYWORD arguments (KurimuzonAkuma fork requirement)
        try:
            logger.debug(f"[DC {dc_id}] Attempt 1: Session(client=client, dc_id={dc_id}, auth_key=..., test_mode={test_mode}, is_media=True)")
            media_session = Session(
                client=client,
                dc_id=dc_id,
                auth_key=auth_key,
                test_mode=test_mode,
                is_media=True,
            )
            await media_session.start()
            logger.info(f"[DC {dc_id}] ✓ Session created successfully with KEYWORD args and is_media=True")
            return media_session
        except TypeError as e:
            logger.warning(f"[DC {dc_id}] ✗ TypeError with keyword args and is_media=True: {e}")
        except Exception as e:
            logger.error(f"[DC {dc_id}] ✗ Failed with keyword args and is_media=True: {type(e).__name__}: {e}")
            raise

        # Attempt 2: Without is_media parameter (fallback)
        try:
            logger.debug(f"[DC {dc_id}] Attempt 2: Session(client=client, dc_id={dc_id}, auth_key=..., test_mode={test_mode})")
            media_session = Session(
                client=client,
                dc_id=dc_id,
                auth_key=auth_key,
                test_mode=test_mode
            )
            await media_session.start()
            logger.info(f"[DC {dc_id}] ✓ Session created successfully with KEYWORD args (no is_media)")
            return media_session
        except TypeError as e:
            logger.error(f"[DC {dc_id}] ✗ TypeError with keyword args (no is_media): {e}")
        except Exception as e:
            logger.error(f"[DC {dc_id}] ✗ Failed with keyword args (no is_media): {type(e).__name__}: {e}")
            raise

        # Attempt 3: Positional arguments as last resort
        try:
            logger.debug(f"[DC {dc_id}] Attempt 3: Session(client, {dc_id}, auth_key, {test_mode}, is_media=True) [POSITIONAL]")
            media_session = Session(
                client,
                dc_id,
                auth_key,
                test_mode,
                is_media=True,
            )
            await media_session.start()
            logger.info(f"[DC {dc_id}] ✓ Session created successfully with POSITIONAL args")
            return media_session
        except TypeError as e:
            logger.error(f"[DC {dc_id}] ✗ TypeError with positional args: {e}")
            error_msg = (
                f"CRITICAL: Failed to create Session for DC {dc_id} after 3 attempts. "
                f"Tried: keyword args with is_media, keyword args without is_media, positional args. "
                f"Last error: {e}. "
                f"Session class: {Session.__module__}.{Session.__name__}. "
                f"This indicates a Pyrogram API incompatibility."
            )
            raise TypeError(error_msg) from e
        except Exception as e:
            logger.error(f"[DC {dc_id}] ✗ Failed with positional args: {type(e).__name__}: {e}")
            raise

    async def generate_media_session(self, client: Client, file_id: FileId) -> Session:
        """
        Generates the media session for the DC that contains the media file.
        This is required for getting the bytes from Telegram servers.

        Fixed for KurimuzonAkuma Pyrogram fork compatibility.
        Includes robust validation and fallback mechanisms.
        """
        dc_id = file_id.dc_id
        media_session = client.media_sessions.get(dc_id, None)

        # Strict validation of cached session
        if media_session is not None:
            logger.debug(f"Found cached media session for DC {dc_id}, validating...")

            is_valid = True
            validation_errors = []

            try:
                # Check 1: Has auth_key attribute
                if not hasattr(media_session, 'auth_key'):
                    is_valid = False
                    validation_errors.append("missing auth_key attribute")
                elif media_session.auth_key is None:
                    is_valid = False
                    validation_errors.append("auth_key is None")

                # Check 2: Has test_mode attribute
                if not hasattr(media_session, 'test_mode'):
                    is_valid = False
                    validation_errors.append("missing test_mode attribute")

                # Check 3: Session is started
                if not hasattr(media_session, 'is_started'):
                    is_valid = False
                    validation_errors.append("missing is_started attribute")
                elif hasattr(media_session, 'is_started') and not media_session.is_started:
                    is_valid = False
                    validation_errors.append("session not started")

            except Exception as e:
                is_valid = False
                validation_errors.append(f"exception during validation: {e}")

            if not is_valid:
                logger.warning(
                    f"Cached session for DC {dc_id} is INVALID (reasons: {', '.join(validation_errors)}). "
                    f"Forcing recreation..."
                )
                try:
                    await media_session.stop()
                except:
                    pass

                if dc_id in client.media_sessions:
                    del client.media_sessions[dc_id]
                media_session = None
            else:
                logger.debug(f"Cached session for DC {dc_id} passed all validation checks ✓")

        if media_session is None:
            logger.info(f"Creating new media session for DC {dc_id}")

            # Get test_mode from client storage (defaults to False for production)
            try:
                test_mode = await client.storage.test_mode()
                logger.debug(f"Retrieved test_mode from client storage: {test_mode}")
            except Exception as e:
                logger.warning(f"Failed to get test_mode from storage, defaulting to False: {e}")
                test_mode = False

            client_dc_id = await client.storage.dc_id()
            logger.debug(f"Client DC: {client_dc_id}, File DC: {dc_id}")

            if dc_id != client_dc_id:
                # Need to create auth for different DC
                logger.info(f"File is on different DC ({dc_id}), creating new auth...")

                # Get DC address for the target DC
                dc_addresses = DC_ADDRESSES_TEST if test_mode else DC_ADDRESSES

                if dc_id in dc_addresses:
                    server_address, port = dc_addresses[dc_id]
                else:
                    # Fallback to default address pattern
                    server_address = f"149.154.167.{40 + dc_id}"
                    port = 443

                logger.info(f"Creating auth for DC {dc_id} at {server_address}:{port} (test_mode={test_mode})")

                try:
                    # New Auth signature: Auth(client, dc_id, server_address, port, test_mode)
                    auth = Auth(
                        client,
                        dc_id,
                        server_address,
                        port,
                        test_mode
                    )
                    auth_key = await auth.create()
                    logger.info(f"Auth key created for DC {dc_id}, length: {len(auth_key)}")
                except Exception as e:
                    logger.error(f"CRITICAL: Failed to create auth for DC {dc_id}: {type(e).__name__}: {e}")
                    raise

                # Create session with robust fallback
                media_session = await self._create_session_with_fallback(
                    client,
                    dc_id,
                    auth_key,
                    test_mode,
                    context="different DC"
                )

                # Export and import authorization
                logger.info(f"Exporting and importing authorization for DC {dc_id}")
                for attempt in range(6):
                    try:
                        exported_auth = await client.invoke(
                            raw.functions.auth.ExportAuthorization(dc_id=dc_id)
                        )

                        await media_session.invoke(
                            raw.functions.auth.ImportAuthorization(
                                id=exported_auth.id, bytes=exported_auth.bytes
                            )
                        )
                        logger.info(f"✓ Successfully imported auth for DC {dc_id} on attempt {attempt + 1}")
                        break
                    except AuthBytesInvalid:
                        logger.warning(f"Invalid authorization bytes for DC {dc_id}, attempt {attempt + 1}/6")
                        if attempt == 5:
                            await media_session.stop()
                            raise AuthBytesInvalid
                        continue
                    except Exception as e:
                        logger.error(f"Auth import error for DC {dc_id} on attempt {attempt + 1}/6: {type(e).__name__}: {e}")
                        if attempt == 5:
                            await media_session.stop()
                            raise
            else:
                # Same DC as client, use existing auth key
                logger.info(f"File is on same DC as client ({dc_id}), reusing client auth key")

                try:
                    auth_key = await client.storage.auth_key()
                    logger.debug(f"Retrieved auth_key from client storage, length: {len(auth_key) if auth_key else 0}")

                    if not auth_key:
                        raise ValueError(f"Client storage returned empty auth_key for DC {dc_id}")

                    # Create session with robust fallback
                    media_session = await self._create_session_with_fallback(
                        client,
                        dc_id,
                        auth_key,
                        test_mode,
                        context="same DC"
                    )

                except Exception as e:
                    logger.error(f"CRITICAL: Failed to create session for same DC {dc_id}: {type(e).__name__}: {e}")
                    raise

            # Cache the newly created session
            logger.info(f"Caching media session for DC {dc_id}")
            client.media_sessions[dc_id] = media_session
        else:
            logger.info(f"Using validated cached media session for DC {dc_id}")

        return media_session

    @staticmethod
    async def get_location(
        file_id: FileId,
    ) -> Union[
        raw.types.InputPhotoFileLocation,
        raw.types.InputDocumentFileLocation,
        raw.types.InputPeerPhotoFileLocation,
    ]:
        """
        Returns the file location for the media file.
        Handles different file types (photos, documents, chat photos).
        """
        file_type = file_id.file_type

        if file_type == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(
                    user_id=file_id.chat_id, access_hash=file_id.chat_access_hash
                )
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(
                        channel_id=utils.get_channel_id(file_id.chat_id),
                        access_hash=file_id.chat_access_hash,
                    )

            location = raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )
        elif file_type == FileType.PHOTO:
            location = raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        else:
            location = raw.types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        return location

    async def yield_file(
        self,
        file_id: FileId,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
    ):
        """
        Async generator that yields the bytes of the media file.
        
        Optimized for video streaming with:
        - Configurable chunk sizes (default 1MB for optimal streaming)
        - Proper byte slicing for range requests
        - Error handling for network issues
        
        Args:
            file_id: The Telegram file ID object
            offset: Starting byte offset (aligned to chunk_size)
            first_part_cut: Bytes to skip in first chunk
            last_part_cut: Bytes to include in last chunk
            part_count: Total number of chunks to stream
            chunk_size: Size of each chunk in bytes
        """
        client = self.client
        logger.debug(f"Starting file stream: offset={offset}, parts={part_count}, chunk_size={chunk_size}")
        
        try:
            media_session = await self.generate_media_session(client, file_id)
        except Exception as e:
            logger.error(f"Failed to generate media session: {e}")
            raise

        current_part = 1
        location = await self.get_location(file_id)

        try:
            r = await media_session.invoke(
                raw.functions.upload.GetFile(
                    location=location, offset=offset, limit=chunk_size
                ),
            )
            
            if isinstance(r, raw.types.upload.File):
                while True:
                    chunk = r.bytes
                    if not chunk:
                        logger.debug(f"Empty chunk received at part {current_part}")
                        break
                    
                    # Apply byte slicing based on position
                    if part_count == 1:
                        # Single chunk: slice both start and end
                        yield chunk[first_part_cut:last_part_cut]
                    elif current_part == 1:
                        # First chunk: slice start only
                        yield chunk[first_part_cut:]
                    elif current_part == part_count:
                        # Last chunk: slice end only
                        yield chunk[:last_part_cut]
                    else:
                        # Middle chunk: yield full chunk
                        yield chunk

                    current_part += 1
                    offset += chunk_size

                    if current_part > part_count:
                        break

                    # Fetch next chunk
                    r = await media_session.invoke(
                        raw.functions.upload.GetFile(
                            location=location, offset=offset, limit=chunk_size
                        ),
                    )
            else:
                logger.warning(f"Unexpected response type: {type(r)}")
                
        except (TimeoutError, AttributeError) as e:
            logger.warning(f"Stream interrupted: {e}")
        except Exception as e:
            logger.error(f"Stream error: {e}")
            raise
        finally:
            logger.debug(f"Finished streaming file: {current_part - 1} parts delivered")

    async def clean_cache(self) -> None:
        """
        Periodically clean the file ID cache to reduce memory usage.
        Runs every 30 minutes.
        """
        while True:
            await asyncio.sleep(self.clean_timer)
            cache_size = len(self.cached_file_ids)
            self.cached_file_ids.clear()
            logger.debug(f"Cleaned file ID cache: {cache_size} entries removed")
