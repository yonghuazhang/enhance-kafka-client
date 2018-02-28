package org.apache.kafka.clients.enhance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public final class Utility {
	private static final Logger logger = LoggerFactory.getLogger(Utility.class);
	public static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	public static final long INVALID_TIMESTAMP = -1L;
	private static final Pattern NORMALIZE_TOPIC_NAME_PATTERN = Pattern.compile("[^a-zA-Z0-9\\.\\-\\_]+");
	private static final String TOPIC_JOIN_CHAR = "_";

	public static int getCpuCores() {
		return Runtime.getRuntime().availableProcessors();
	}

	public static boolean isInvalidString(String value) {
		return (null == value) || value.isEmpty() || value.trim().isEmpty();
	}

	public static long convertTimeByString(String dateString) throws ParseException {
		return Utility.SIMPLE_DATE_FORMAT.parse(dateString).getTime();
	}

	public static boolean fileExists(String fileName) {
		return Files.exists(new File(fileName).toPath());
	}

	public static File createFile(String fileName) throws IOException {
		File file = new File(fileName);
		if (!file.exists()) {
			if (!file.getParentFile().exists()) {
				file.getParentFile().mkdirs();
			}
			file.createNewFile();
		}
		return file;
	}

	public static boolean moveFile(String fromFn, String ToFn) {
		try {
			Path fromPath = (new File(fromFn)).toPath();
			Path toPath = (new File(ToFn)).toPath();
			Files.move(fromPath, toPath, REPLACE_EXISTING);
			return true;
		} catch (IOException e) {
			logger.warn("move file failed. due to ", e);
		}
		return false;
	}

	public static String normalizeTopicName(String topicName) {
		return NORMALIZE_TOPIC_NAME_PATTERN.matcher(topicName).replaceAll(TOPIC_JOIN_CHAR);
	}

	public static void sleep(long durationMs) {
		try {
			TimeUnit.MILLISECONDS.sleep(durationMs);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	public static String getLocalAddress() {
		try {
			// Traversal Network interface to get the first non-loopback and non-private address
			Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
			ArrayList<String> ipv4Result = new ArrayList<String>();
			ArrayList<String> ipv6Result = new ArrayList<String>();
			while (enumeration.hasMoreElements()) {
				final NetworkInterface networkInterface = enumeration.nextElement();
				final Enumeration<InetAddress> en = networkInterface.getInetAddresses();
				while (en.hasMoreElements()) {
					final InetAddress address = en.nextElement();
					if (!address.isLoopbackAddress()) {
						if (address instanceof Inet6Address) {
							ipv6Result.add(normalizeHostAddress(address));
						} else {
							ipv4Result.add(normalizeHostAddress(address));
						}
					}
				}
			}

			// prefer ipv4
			if (!ipv4Result.isEmpty()) {
				for (String ip : ipv4Result) {
					if (ip.startsWith("127.0") || ip.startsWith("192.168")) {
						continue;
					}

					return ip;
				}

				return ipv4Result.get(ipv4Result.size() - 1);
			} else if (!ipv6Result.isEmpty()) {
				return ipv6Result.get(0);
			}
			//If failed to find,fall back to localhost
			final InetAddress localHost = InetAddress.getLocalHost();
			return normalizeHostAddress(localHost);
		} catch (SocketException e) {
			logger.warn("getLocalAddress lead to SocketException.", e);
		} catch (UnknownHostException e) {
			logger.warn("getLocalAddress lead to UnknownHostException.", e);
		}

		return "unkown_ip";
	}

	public static String normalizeHostAddress(final InetAddress localHost) {
		if (localHost instanceof Inet6Address) {
			return "[" + localHost.getHostAddress() + "]";
		} else {
			return localHost.getHostAddress();
		}
	}
}
