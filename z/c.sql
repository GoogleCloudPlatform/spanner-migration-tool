-- phpMyAdmin SQL Dump
-- version 4.9.5
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1:3306
-- Generation Time: Apr 29, 2021 at 10:52 AM
-- Server version: 10.4.14-MariaDB-cll-lve
-- PHP Version: 7.2.34

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET AUTOCOMMIT = 0;
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `u723062544_UsersDb`
--

-- --------------------------------------------------------

--
-- Table structure for table `wp_actionscheduler_actions`
--

CREATE TABLE `wp_actionscheduler_actions` (
  `action_id` bigint(20) UNSIGNED NOT NULL,
  `hook` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `status` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL,
  `scheduled_date_gmt` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `scheduled_date_local` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `args` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `schedule` longtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `group_id` bigint(20) UNSIGNED NOT NULL DEFAULT 0,
  `attempts` int(11) NOT NULL DEFAULT 0,
  `last_attempt_gmt` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `last_attempt_local` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `claim_id` bigint(20) UNSIGNED NOT NULL DEFAULT 0,
  `extended_args` varchar(8000) COLLATE utf8mb4_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `wp_actionscheduler_actions`
--

INSERT INTO `wp_actionscheduler_actions` (`action_id`, `hook`, `status`, `scheduled_date_gmt`, `scheduled_date_local`, `args`, `schedule`, `group_id`, `attempts`, `last_attempt_gmt`, `last_attempt_local`, `claim_id`, `extended_args`) VALUES
(5, 'action_scheduler/migration_hook', 'complete', '2021-04-14 18:39:40', '2021-04-14 18:39:40', '[]', 'O:30:\"ActionScheduler_SimpleSchedule\":2:{s:22:\"\0*\0scheduled_timestamp\";i:1618425580;s:41:\"\0ActionScheduler_SimpleSchedule\0timestamp\";i:1618425580;}', 1, 1, '2021-04-14 18:41:13', '2021-04-14 18:41:13', 0, NULL),
(10, 'wpforms_process_entry_emails_meta_cleanup', 'complete', '2021-04-15 00:00:00', '2021-04-15 00:00:00', '{\"tasks_meta_id\":1}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1618444800;s:18:\"\0*\0first_timestamp\";i:1618444800;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1618444800;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 3, 1, '2021-04-15 00:22:03', '2021-04-15 00:22:03', 0, NULL),
(11, 'wpforms_email_summaries_fetch_info_blocks', 'complete', '2021-04-17 06:15:52', '2021-04-17 06:15:52', '{\"tasks_meta_id\":null}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1618640152;s:18:\"\0*\0first_timestamp\";i:1618640152;s:13:\"\0*\0recurrence\";i:604800;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1618640152;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:604800;}', 3, 1, '2021-04-17 14:15:38', '2021-04-17 14:15:38', 0, NULL),
(17, 'wpforms_admin_notifications_update', 'complete', '0000-00-00 00:00:00', '0000-00-00 00:00:00', '{\"tasks_meta_id\":2}', 'O:28:\"ActionScheduler_NullSchedule\":0:{}', 3, 1, '2021-04-14 18:49:07', '2021-04-14 18:49:07', 0, NULL),
(18, 'wpforms_admin_addons_cache_update', 'complete', '2021-04-21 18:49:07', '2021-04-21 18:49:07', '{\"tasks_meta_id\":3}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1619030947;s:18:\"\0*\0first_timestamp\";i:1619030947;s:13:\"\0*\0recurrence\";i:604800;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1619030947;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:604800;}', 3, 1, '2021-04-21 19:11:36', '2021-04-21 19:11:36', 0, NULL),
(24, 'wpforms_process_entry_emails_meta_cleanup', 'complete', '2021-04-16 00:22:03', '2021-04-16 00:22:03', '{\"tasks_meta_id\":1}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1618532523;s:18:\"\0*\0first_timestamp\";i:1618444800;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1618532523;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 3, 1, '2021-04-16 01:01:20', '2021-04-16 01:01:20', 0, NULL),
(45, 'wpforms_process_entry_emails_meta_cleanup', 'complete', '2021-04-17 01:01:20', '2021-04-17 01:01:20', '{\"tasks_meta_id\":1}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1618621280;s:18:\"\0*\0first_timestamp\";i:1618444800;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1618621280;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 3, 1, '2021-04-17 02:32:09', '2021-04-17 02:32:09', 0, NULL),
(57, 'wpforms_process_entry_emails_meta_cleanup', 'complete', '2021-04-18 02:32:09', '2021-04-18 02:32:09', '{\"tasks_meta_id\":1}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1618713129;s:18:\"\0*\0first_timestamp\";i:1618444800;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1618713129;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 3, 1, '2021-04-18 05:20:26', '2021-04-18 05:20:26', 0, NULL),
(61, 'wpforms_email_summaries_fetch_info_blocks', 'complete', '2021-04-24 14:15:38', '2021-04-24 14:15:38', '{\"tasks_meta_id\":null}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1619273738;s:18:\"\0*\0first_timestamp\";i:1618640152;s:13:\"\0*\0recurrence\";i:604800;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1619273738;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:604800;}', 3, 1, '2021-04-24 15:43:46', '2021-04-24 15:43:46', 0, NULL),
(65, 'wpforms_process_entry_emails_meta_cleanup', 'complete', '2021-04-19 05:20:26', '2021-04-19 05:20:26', '{\"tasks_meta_id\":1}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1618809626;s:18:\"\0*\0first_timestamp\";i:1618444800;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1618809626;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 3, 1, '2021-04-19 07:34:29', '2021-04-19 07:34:29', 0, NULL),
(78, 'wpforms_process_entry_emails_meta_cleanup', 'complete', '2021-04-20 07:34:29', '2021-04-20 07:34:29', '{\"tasks_meta_id\":1}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1618904069;s:18:\"\0*\0first_timestamp\";i:1618444800;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1618904069;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 3, 1, '2021-04-20 10:32:08', '2021-04-20 10:32:08', 0, NULL),
(84, 'wpforms_process_entry_emails_meta_cleanup', 'complete', '2021-04-21 10:32:08', '2021-04-21 10:32:08', '{\"tasks_meta_id\":1}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1619001128;s:18:\"\0*\0first_timestamp\";i:1618444800;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1619001128;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 3, 1, '2021-04-21 10:39:11', '2021-04-21 10:39:11', 0, NULL),
(101, 'wpforms_process_entry_emails_meta_cleanup', 'complete', '2021-04-22 10:39:11', '2021-04-22 10:39:11', '{\"tasks_meta_id\":1}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1619087951;s:18:\"\0*\0first_timestamp\";i:1618444800;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1619087951;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 3, 1, '2021-04-22 11:00:01', '2021-04-22 11:00:01', 0, NULL),
(120, 'wpforms_admin_addons_cache_update', 'complete', '2021-04-28 19:11:36', '2021-04-28 19:11:36', '{\"tasks_meta_id\":3}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1619637096;s:18:\"\0*\0first_timestamp\";i:1619030947;s:13:\"\0*\0recurrence\";i:604800;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1619637096;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:604800;}', 3, 1, '2021-04-28 19:52:35', '2021-04-28 19:52:35', 0, NULL),
(140, 'wpforms_process_entry_emails_meta_cleanup', 'complete', '2021-04-23 11:00:01', '2021-04-23 11:00:01', '{\"tasks_meta_id\":1}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1619175601;s:18:\"\0*\0first_timestamp\";i:1618444800;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1619175601;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 3, 1, '2021-04-23 13:23:56', '2021-04-23 13:23:56', 0, NULL),
(148, 'wpforms_process_entry_emails_meta_cleanup', 'complete', '2021-04-24 13:23:56', '2021-04-24 13:23:56', '{\"tasks_meta_id\":1}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1619270636;s:18:\"\0*\0first_timestamp\";i:1618444800;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1619270636;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 3, 1, '2021-04-24 13:29:44', '2021-04-24 13:29:44', 0, NULL),
(203, 'wpforms_process_entry_emails_meta_cleanup', 'complete', '2021-04-25 13:29:44', '2021-04-25 13:29:44', '{\"tasks_meta_id\":1}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1619357384;s:18:\"\0*\0first_timestamp\";i:1618444800;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1619357384;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 3, 1, '2021-04-25 13:35:11', '2021-04-25 13:35:11', 0, NULL),
(206, 'wpforms_email_summaries_fetch_info_blocks', 'pending', '2021-05-01 15:43:46', '2021-05-01 15:43:46', '{\"tasks_meta_id\":null}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1619883826;s:18:\"\0*\0first_timestamp\";i:1618640152;s:13:\"\0*\0recurrence\";i:604800;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1619883826;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:604800;}', 3, 0, '0000-00-00 00:00:00', '0000-00-00 00:00:00', 0, NULL),
(257, 'wpforms_process_entry_emails_meta_cleanup', 'complete', '2021-04-26 13:35:11', '2021-04-26 13:35:11', '{\"tasks_meta_id\":1}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1619444111;s:18:\"\0*\0first_timestamp\";i:1618444800;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1619444111;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 3, 1, '2021-04-26 13:37:19', '2021-04-26 13:37:19', 0, NULL),
(279, 'aioseo_send_usage_data', 'pending', '2021-05-03 02:15:00', '2021-05-03 02:15:00', '[]', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1620008100;s:18:\"\0*\0first_timestamp\";i:1618776039;s:13:\"\0*\0recurrence\";i:604800;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1620008100;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:604800;}', 2, 0, '0000-00-00 00:00:00', '0000-00-00 00:00:00', 0, NULL),
(302, 'wpforms_process_entry_emails_meta_cleanup', 'complete', '2021-04-27 13:37:19', '2021-04-27 13:37:19', '{\"tasks_meta_id\":1}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1619530639;s:18:\"\0*\0first_timestamp\";i:1618444800;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1619530639;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 3, 1, '2021-04-27 15:54:50', '2021-04-27 15:54:50', 0, NULL),
(309, 'aioseo_cleanup_action_scheduler', 'complete', '2021-04-28 15:54:50', '2021-04-28 15:54:50', '[]', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1619625290;s:18:\"\0*\0first_timestamp\";i:1618512073;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1619625290;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 2, 1, '2021-04-28 18:14:06', '2021-04-28 18:14:06', 0, NULL),
(310, 'wpforms_process_entry_emails_meta_cleanup', 'complete', '2021-04-28 15:54:50', '2021-04-28 15:54:50', '{\"tasks_meta_id\":1}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1619625290;s:18:\"\0*\0first_timestamp\";i:1618444800;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1619625290;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 3, 1, '2021-04-28 18:14:06', '2021-04-28 18:14:06', 0, NULL),
(311, 'aioseo_admin_notifications_update', 'complete', '2021-04-28 15:54:50', '2021-04-28 15:54:50', '[]', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1619625290;s:18:\"\0*\0first_timestamp\";i:1618448400;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1619625290;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 2, 1, '2021-04-28 18:14:07', '2021-04-28 18:14:07', 0, NULL),
(320, 'aioseo_image_sitemap_scan', 'complete', '2021-04-28 11:45:25', '2021-04-28 11:45:25', '[]', 'O:30:\"ActionScheduler_SimpleSchedule\":2:{s:22:\"\0*\0scheduled_timestamp\";i:1619610325;s:41:\"\0ActionScheduler_SimpleSchedule\0timestamp\";i:1619610325;}', 2, 1, '2021-04-28 18:14:07', '2021-04-28 18:14:07', 0, NULL),
(321, 'aioseo_cleanup_action_scheduler', 'pending', '2021-04-29 18:14:06', '2021-04-29 18:14:06', '[]', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1619720046;s:18:\"\0*\0first_timestamp\";i:1618512073;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1619720046;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 2, 0, '0000-00-00 00:00:00', '0000-00-00 00:00:00', 0, NULL),
(322, 'wpforms_process_entry_emails_meta_cleanup', 'pending', '2021-04-29 18:14:06', '2021-04-29 18:14:06', '{\"tasks_meta_id\":1}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1619720046;s:18:\"\0*\0first_timestamp\";i:1618444800;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1619720046;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 3, 0, '0000-00-00 00:00:00', '0000-00-00 00:00:00', 0, NULL),
(323, 'aioseo_admin_notifications_update', 'pending', '2021-04-29 18:14:07', '2021-04-29 18:14:07', '[]', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1619720047;s:18:\"\0*\0first_timestamp\";i:1618448400;s:13:\"\0*\0recurrence\";i:86400;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1619720047;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:86400;}', 2, 0, '0000-00-00 00:00:00', '0000-00-00 00:00:00', 0, NULL),
(324, 'aioseo_image_sitemap_scan', 'complete', '2021-04-28 18:29:07', '2021-04-28 18:29:07', '[]', 'O:30:\"ActionScheduler_SimpleSchedule\":2:{s:22:\"\0*\0scheduled_timestamp\";i:1619634547;s:41:\"\0ActionScheduler_SimpleSchedule\0timestamp\";i:1619634547;}', 2, 1, '2021-04-28 19:02:49', '2021-04-28 19:02:49', 0, NULL),
(325, 'aioseo_image_sitemap_scan', 'complete', '2021-04-28 19:17:49', '2021-04-28 19:17:49', '[]', 'O:30:\"ActionScheduler_SimpleSchedule\":2:{s:22:\"\0*\0scheduled_timestamp\";i:1619637469;s:41:\"\0ActionScheduler_SimpleSchedule\0timestamp\";i:1619637469;}', 2, 1, '2021-04-28 19:52:35', '2021-04-28 19:52:35', 0, NULL),
(326, 'wpforms_admin_addons_cache_update', 'pending', '2021-05-05 19:52:35', '2021-05-05 19:52:35', '{\"tasks_meta_id\":3}', 'O:32:\"ActionScheduler_IntervalSchedule\":5:{s:22:\"\0*\0scheduled_timestamp\";i:1620244355;s:18:\"\0*\0first_timestamp\";i:1619030947;s:13:\"\0*\0recurrence\";i:604800;s:49:\"\0ActionScheduler_IntervalSchedule\0start_timestamp\";i:1620244355;s:53:\"\0ActionScheduler_IntervalSchedule\0interval_in_seconds\";i:604800;}', 3, 0, '0000-00-00 00:00:00', '0000-00-00 00:00:00', 0, NULL),
(327, 'aioseo_image_sitemap_scan', 'complete', '2021-04-28 20:07:35', '2021-04-28 20:07:35', '[]', 'O:30:\"ActionScheduler_SimpleSchedule\":2:{s:22:\"\0*\0scheduled_timestamp\";i:1619640455;s:41:\"\0ActionScheduler_SimpleSchedule\0timestamp\";i:1619640455;}', 2, 1, '2021-04-28 20:25:33', '2021-04-28 20:25:33', 0, NULL),
(328, 'aioseo_image_sitemap_scan', 'complete', '2021-04-28 20:40:33', '2021-04-28 20:40:33', '[]', 'O:30:\"ActionScheduler_SimpleSchedule\":2:{s:22:\"\0*\0scheduled_timestamp\";i:1619642433;s:41:\"\0ActionScheduler_SimpleSchedule\0timestamp\";i:1619642433;}', 2, 1, '2021-04-28 20:59:11', '2021-04-28 20:59:11', 0, NULL),
(329, 'aioseo_image_sitemap_scan', 'complete', '2021-04-28 21:14:11', '2021-04-28 21:14:11', '[]', 'O:30:\"ActionScheduler_SimpleSchedule\":2:{s:22:\"\0*\0scheduled_timestamp\";i:1619644451;s:41:\"\0ActionScheduler_SimpleSchedule\0timestamp\";i:1619644451;}', 2, 1, '2021-04-28 22:52:53', '2021-04-28 22:52:53', 0, NULL),
(330, 'aioseo_image_sitemap_scan', 'complete', '2021-04-28 23:07:53', '2021-04-28 23:07:53', '[]', 'O:30:\"ActionScheduler_SimpleSchedule\":2:{s:22:\"\0*\0scheduled_timestamp\";i:1619651273;s:41:\"\0ActionScheduler_SimpleSchedule\0timestamp\";i:1619651273;}', 2, 1, '2021-04-28 23:09:32', '2021-04-28 23:09:32', 0, NULL),
(331, 'aioseo_image_sitemap_scan', 'complete', '2021-04-28 23:24:32', '2021-04-28 23:24:32', '[]', 'O:30:\"ActionScheduler_SimpleSchedule\":2:{s:22:\"\0*\0scheduled_timestamp\";i:1619652272;s:41:\"\0ActionScheduler_SimpleSchedule\0timestamp\";i:1619652272;}', 2, 1, '2021-04-29 02:32:08', '2021-04-29 02:32:08', 0, NULL),
(332, 'aioseo_image_sitemap_scan', 'complete', '2021-04-29 02:47:08', '2021-04-29 02:47:08', '[]', 'O:30:\"ActionScheduler_SimpleSchedule\":2:{s:22:\"\0*\0scheduled_timestamp\";i:1619664428;s:41:\"\0ActionScheduler_SimpleSchedule\0timestamp\";i:1619664428;}', 2, 1, '2021-04-29 09:52:49', '2021-04-29 09:52:49', 0, NULL),
(333, 'aioseo_image_sitemap_scan', 'pending', '2021-04-29 10:07:49', '2021-04-29 10:07:49', '[]', 'O:30:\"ActionScheduler_SimpleSchedule\":2:{s:22:\"\0*\0scheduled_timestamp\";i:1619690869;s:41:\"\0ActionScheduler_SimpleSchedule\0timestamp\";i:1619690869;}', 2, 0, '0000-00-00 00:00:00', '0000-00-00 00:00:00', 0, NULL);

-- --------------------------------------------------------

--
-- Table structure for table `wp_actionscheduler_claims`
--

CREATE TABLE `wp_actionscheduler_claims` (
  `claim_id` bigint(20) UNSIGNED NOT NULL,
  `date_created_gmt` datetime NOT NULL DEFAULT '0000-00-00 00:00:00'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Table structure for table `wp_actionscheduler_groups`
--

CREATE TABLE `wp_actionscheduler_groups` (
  `group_id` bigint(20) UNSIGNED NOT NULL,
  `slug` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `wp_actionscheduler_groups`
--

INSERT INTO `wp_actionscheduler_groups` (`group_id`, `slug`) VALUES
(1, 'action-scheduler-migration'),
(2, 'aioseo'),
(3, 'wpforms');

-- --------------------------------------------------------

--
-- Table structure for table `wp_actionscheduler_logs`
--

CREATE TABLE `wp_actionscheduler_logs` (
  `log_id` bigint(20) UNSIGNED NOT NULL,
  `action_id` bigint(20) UNSIGNED NOT NULL,
  `message` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `log_date_gmt` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `log_date_local` datetime NOT NULL DEFAULT '0000-00-00 00:00:00'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `wp_actionscheduler_logs`
--

INSERT INTO `wp_actionscheduler_logs` (`log_id`, `action_id`, `message`, `log_date_gmt`, `log_date_local`) VALUES
(1, 5, 'action created', '2021-04-14 18:38:40', '2021-04-14 18:38:40'),
(6, 5, 'action started via WP Cron', '2021-04-14 18:41:13', '2021-04-14 18:41:13'),
(7, 5, 'action complete via WP Cron', '2021-04-14 18:41:13', '2021-04-14 18:41:13'),
(12, 10, 'action created', '2021-04-14 18:41:14', '2021-04-14 18:41:14'),
(13, 11, 'action created', '2021-04-14 18:41:14', '2021-04-14 18:41:14'),
(25, 17, 'action created', '2021-04-14 18:49:06', '2021-04-14 18:49:06'),
(26, 18, 'action created', '2021-04-14 18:49:07', '2021-04-14 18:49:07'),
(27, 17, 'action started via Async Request', '2021-04-14 18:49:07', '2021-04-14 18:49:07'),
(28, 17, 'action complete via Async Request', '2021-04-14 18:49:07', '2021-04-14 18:49:07'),
(44, 10, 'action started via WP Cron', '2021-04-15 00:22:03', '2021-04-15 00:22:03'),
(45, 10, 'action complete via WP Cron', '2021-04-15 00:22:03', '2021-04-15 00:22:03'),
(46, 24, 'action created', '2021-04-15 00:22:03', '2021-04-15 00:22:03'),
(107, 24, 'action started via WP Cron', '2021-04-16 01:01:20', '2021-04-16 01:01:20'),
(108, 24, 'action complete via WP Cron', '2021-04-16 01:01:20', '2021-04-16 01:01:20'),
(109, 45, 'action created', '2021-04-16 01:01:20', '2021-04-16 01:01:20'),
(143, 45, 'action started via WP Cron', '2021-04-17 02:32:09', '2021-04-17 02:32:09'),
(144, 45, 'action complete via WP Cron', '2021-04-17 02:32:09', '2021-04-17 02:32:09'),
(145, 57, 'action created', '2021-04-17 02:32:09', '2021-04-17 02:32:09'),
(155, 11, 'action started via WP Cron', '2021-04-17 14:15:37', '2021-04-17 14:15:37'),
(156, 11, 'action complete via WP Cron', '2021-04-17 14:15:38', '2021-04-17 14:15:38'),
(157, 61, 'action created', '2021-04-17 14:15:38', '2021-04-17 14:15:38'),
(167, 57, 'action started via WP Cron', '2021-04-18 05:20:26', '2021-04-18 05:20:26'),
(168, 57, 'action complete via WP Cron', '2021-04-18 05:20:26', '2021-04-18 05:20:26'),
(169, 65, 'action created', '2021-04-18 05:20:26', '2021-04-18 05:20:26'),
(206, 65, 'action started via WP Cron', '2021-04-19 07:34:29', '2021-04-19 07:34:29'),
(207, 65, 'action complete via WP Cron', '2021-04-19 07:34:29', '2021-04-19 07:34:29'),
(208, 78, 'action created', '2021-04-19 07:34:29', '2021-04-19 07:34:29'),
(224, 78, 'action started via WP Cron', '2021-04-20 10:32:08', '2021-04-20 10:32:08'),
(225, 78, 'action complete via WP Cron', '2021-04-20 10:32:08', '2021-04-20 10:32:08'),
(226, 84, 'action created', '2021-04-20 10:32:08', '2021-04-20 10:32:08'),
(275, 84, 'action started via WP Cron', '2021-04-21 10:39:11', '2021-04-21 10:39:11'),
(276, 84, 'action complete via WP Cron', '2021-04-21 10:39:11', '2021-04-21 10:39:11'),
(277, 101, 'action created', '2021-04-21 10:39:11', '2021-04-21 10:39:11'),
(332, 18, 'action started via WP Cron', '2021-04-21 19:11:36', '2021-04-21 19:11:36'),
(333, 18, 'action complete via WP Cron', '2021-04-21 19:11:36', '2021-04-21 19:11:36'),
(334, 120, 'action created', '2021-04-21 19:11:36', '2021-04-21 19:11:36'),
(392, 101, 'action started via WP Cron', '2021-04-22 11:00:01', '2021-04-22 11:00:01'),
(393, 101, 'action complete via WP Cron', '2021-04-22 11:00:01', '2021-04-22 11:00:01'),
(394, 140, 'action created', '2021-04-22 11:00:01', '2021-04-22 11:00:01'),
(416, 140, 'action started via WP Cron', '2021-04-23 13:23:56', '2021-04-23 13:23:56'),
(417, 140, 'action complete via WP Cron', '2021-04-23 13:23:56', '2021-04-23 13:23:56'),
(418, 148, 'action created', '2021-04-23 13:23:56', '2021-04-23 13:23:56'),
(581, 148, 'action started via WP Cron', '2021-04-24 13:29:44', '2021-04-24 13:29:44'),
(582, 148, 'action complete via WP Cron', '2021-04-24 13:29:44', '2021-04-24 13:29:44'),
(583, 203, 'action created', '2021-04-24 13:29:44', '2021-04-24 13:29:44'),
(590, 61, 'action started via WP Cron', '2021-04-24 15:43:46', '2021-04-24 15:43:46'),
(591, 61, 'action complete via WP Cron', '2021-04-24 15:43:46', '2021-04-24 15:43:46'),
(592, 206, 'action created', '2021-04-24 15:43:46', '2021-04-24 15:43:46'),
(743, 203, 'action started via WP Cron', '2021-04-25 13:35:11', '2021-04-25 13:35:11'),
(744, 203, 'action complete via WP Cron', '2021-04-25 13:35:11', '2021-04-25 13:35:11'),
(745, 257, 'action created', '2021-04-25 13:35:11', '2021-04-25 13:35:11'),
(811, 279, 'action created', '2021-04-26 02:15:00', '2021-04-26 02:15:00'),
(878, 257, 'action started via WP Cron', '2021-04-26 13:37:19', '2021-04-26 13:37:19'),
(879, 257, 'action complete via WP Cron', '2021-04-26 13:37:19', '2021-04-26 13:37:19'),
(880, 302, 'action created', '2021-04-26 13:37:19', '2021-04-26 13:37:19'),
(901, 309, 'action created', '2021-04-27 15:54:50', '2021-04-27 15:54:50'),
(902, 302, 'action started via WP Cron', '2021-04-27 15:54:50', '2021-04-27 15:54:50'),
(903, 302, 'action complete via WP Cron', '2021-04-27 15:54:50', '2021-04-27 15:54:50'),
(904, 310, 'action created', '2021-04-27 15:54:50', '2021-04-27 15:54:50'),
(907, 311, 'action created', '2021-04-27 15:54:50', '2021-04-27 15:54:50'),
(933, 320, 'action created', '2021-04-28 11:30:25', '2021-04-28 11:30:25'),
(935, 309, 'action started via WP Cron', '2021-04-28 18:14:05', '2021-04-28 18:14:05'),
(936, 309, 'action complete via WP Cron', '2021-04-28 18:14:05', '2021-04-28 18:14:05'),
(937, 321, 'action created', '2021-04-28 18:14:06', '2021-04-28 18:14:06'),
(938, 310, 'action started via WP Cron', '2021-04-28 18:14:06', '2021-04-28 18:14:06'),
(939, 310, 'action complete via WP Cron', '2021-04-28 18:14:06', '2021-04-28 18:14:06'),
(940, 322, 'action created', '2021-04-28 18:14:07', '2021-04-28 18:14:07'),
(941, 311, 'action started via WP Cron', '2021-04-28 18:14:07', '2021-04-28 18:14:07'),
(942, 311, 'action complete via WP Cron', '2021-04-28 18:14:07', '2021-04-28 18:14:07'),
(943, 323, 'action created', '2021-04-28 18:14:07', '2021-04-28 18:14:07'),
(944, 320, 'action started via WP Cron', '2021-04-28 18:14:07', '2021-04-28 18:14:07'),
(945, 324, 'action created', '2021-04-28 18:14:07', '2021-04-28 18:14:07'),
(946, 320, 'action complete via WP Cron', '2021-04-28 18:14:07', '2021-04-28 18:14:07'),
(947, 324, 'action started via WP Cron', '2021-04-28 19:02:49', '2021-04-28 19:02:49'),
(948, 325, 'action created', '2021-04-28 19:02:49', '2021-04-28 19:02:49'),
(949, 324, 'action complete via WP Cron', '2021-04-28 19:02:49', '2021-04-28 19:02:49'),
(950, 120, 'action started via WP Cron', '2021-04-28 19:52:35', '2021-04-28 19:52:35'),
(951, 120, 'action complete via WP Cron', '2021-04-28 19:52:35', '2021-04-28 19:52:35'),
(952, 326, 'action created', '2021-04-28 19:52:35', '2021-04-28 19:52:35'),
(953, 325, 'action started via WP Cron', '2021-04-28 19:52:35', '2021-04-28 19:52:35'),
(954, 327, 'action created', '2021-04-28 19:52:35', '2021-04-28 19:52:35'),
(955, 325, 'action complete via WP Cron', '2021-04-28 19:52:35', '2021-04-28 19:52:35'),
(956, 327, 'action started via WP Cron', '2021-04-28 20:25:33', '2021-04-28 20:25:33'),
(957, 328, 'action created', '2021-04-28 20:25:33', '2021-04-28 20:25:33'),
(958, 327, 'action complete via WP Cron', '2021-04-28 20:25:33', '2021-04-28 20:25:33'),
(959, 328, 'action started via WP Cron', '2021-04-28 20:59:11', '2021-04-28 20:59:11'),
(960, 329, 'action created', '2021-04-28 20:59:11', '2021-04-28 20:59:11'),
(961, 328, 'action complete via WP Cron', '2021-04-28 20:59:11', '2021-04-28 20:59:11'),
(962, 329, 'action started via WP Cron', '2021-04-28 22:52:53', '2021-04-28 22:52:53'),
(963, 330, 'action created', '2021-04-28 22:52:53', '2021-04-28 22:52:53'),
(964, 329, 'action complete via WP Cron', '2021-04-28 22:52:53', '2021-04-28 22:52:53'),
(965, 330, 'action started via WP Cron', '2021-04-28 23:09:32', '2021-04-28 23:09:32'),
(966, 331, 'action created', '2021-04-28 23:09:32', '2021-04-28 23:09:32'),
(967, 330, 'action complete via WP Cron', '2021-04-28 23:09:32', '2021-04-28 23:09:32'),
(968, 331, 'action started via WP Cron', '2021-04-29 02:32:08', '2021-04-29 02:32:08'),
(969, 332, 'action created', '2021-04-29 02:32:08', '2021-04-29 02:32:08'),
(970, 331, 'action complete via WP Cron', '2021-04-29 02:32:08', '2021-04-29 02:32:08'),
(971, 332, 'action started via WP Cron', '2021-04-29 09:52:49', '2021-04-29 09:52:49'),
(972, 333, 'action created', '2021-04-29 09:52:49', '2021-04-29 09:52:49'),
(973, 332, 'action complete via WP Cron', '2021-04-29 09:52:49', '2021-04-29 09:52:49');

-- --------------------------------------------------------

--
-- Table structure for table `wp_aioseo_notifications`
--

CREATE TABLE `wp_aioseo_notifications` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `slug` varchar(13) COLLATE utf8mb4_unicode_ci NOT NULL,
  `title` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `content` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
  `type` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `level` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `notification_id` bigint(20) UNSIGNED DEFAULT NULL,
  `notification_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `start` datetime DEFAULT NULL,
  `end` datetime DEFAULT NULL,
  `button1_label` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `button1_action` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `button2_label` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `button2_action` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `dismissed` tinyint(1) NOT NULL DEFAULT 0,
  `created` datetime NOT NULL,
  `updated` datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Table structure for table `wp_aioseo_posts`
--

CREATE TABLE `wp_aioseo_posts` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `post_id` bigint(20) UNSIGNED NOT NULL,
  `title` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `description` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `keywords` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `keyphrases` longtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `page_analysis` longtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `canonical_url` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `og_title` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `og_description` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `og_object_type` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT 'default',
  `og_image_type` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT 'default',
  `og_image_custom_url` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `og_image_custom_fields` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `og_custom_image_width` int(11) DEFAULT NULL,
  `og_custom_image_height` int(11) DEFAULT NULL,
  `og_video` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `og_custom_url` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `og_article_section` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `og_article_tags` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `twitter_use_og` tinyint(1) DEFAULT 0,
  `twitter_card` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT 'default',
  `twitter_image_type` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT 'default',
  `twitter_image_custom_url` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `twitter_image_custom_fields` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `twitter_title` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `twitter_description` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `seo_score` int(11) NOT NULL DEFAULT 0,
  `schema_type` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `schema_type_options` longtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `pillar_content` tinyint(1) DEFAULT NULL,
  `robots_default` tinyint(1) NOT NULL DEFAULT 1,
  `robots_noindex` tinyint(1) NOT NULL DEFAULT 0,
  `robots_noarchive` tinyint(1) NOT NULL DEFAULT 0,
  `robots_nosnippet` tinyint(1) NOT NULL DEFAULT 0,
  `robots_nofollow` tinyint(1) NOT NULL DEFAULT 0,
  `robots_noimageindex` tinyint(1) NOT NULL DEFAULT 0,
  `robots_noodp` tinyint(1) NOT NULL DEFAULT 0,
  `robots_notranslate` tinyint(1) NOT NULL DEFAULT 0,
  `robots_max_snippet` int(11) DEFAULT NULL,
  `robots_max_videopreview` int(11) DEFAULT NULL,
  `robots_max_imagepreview` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT 'large',
  `tabs` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `images` longtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `image_scan_date` datetime DEFAULT NULL,
  `priority` tinytext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `frequency` tinytext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `videos` longtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `video_thumbnail` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `video_scan_date` datetime DEFAULT NULL,
  `local_seo` longtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created` datetime NOT NULL,
  `updated` datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `wp_aioseo_posts`
--

INSERT INTO `wp_aioseo_posts` (`id`, `post_id`, `title`, `description`, `keywords`, `keyphrases`, `page_analysis`, `canonical_url`, `og_title`, `og_description`, `og_object_type`, `og_image_type`, `og_image_custom_url`, `og_image_custom_fields`, `og_custom_image_width`, `og_custom_image_height`, `og_video`, `og_custom_url`, `og_article_section`, `og_article_tags`, `twitter_use_og`, `twitter_card`, `twitter_image_type`, `twitter_image_custom_url`, `twitter_image_custom_fields`, `twitter_title`, `twitter_description`, `seo_score`, `schema_type`, `schema_type_options`, `pillar_content`, `robots_default`, `robots_noindex`, `robots_noarchive`, `robots_nosnippet`, `robots_nofollow`, `robots_noimageindex`, `robots_noodp`, `robots_notranslate`, `robots_max_snippet`, `robots_max_videopreview`, `robots_max_imagepreview`, `tabs`, `images`, `image_scan_date`, `priority`, `frequency`, `videos`, `video_thumbnail`, `video_scan_date`, `local_seo`, `created`, `updated`) VALUES
(1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'default', 'default', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 'default', 'default', NULL, NULL, NULL, NULL, 0, NULL, NULL, 0, 1, 0, 0, 0, 0, 0, 0, 0, NULL, NULL, 'large', NULL, NULL, '2021-04-14 18:42:52', NULL, NULL, NULL, NULL, NULL, NULL, '2021-04-14 18:42:52', '2021-04-14 18:42:52'),
(2, 2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'default', 'default', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 'default', 'default', NULL, NULL, NULL, NULL, 0, NULL, NULL, 0, 1, 0, 0, 0, 0, 0, 0, 0, NULL, NULL, 'large', NULL, NULL, '2021-04-14 18:42:52', NULL, NULL, NULL, NULL, NULL, NULL, '2021-04-14 18:42:52', '2021-04-14 18:42:52');

-- --------------------------------------------------------

--
-- Table structure for table `wp_commentmeta`
--

CREATE TABLE `wp_commentmeta` (
  `meta_id` bigint(20) UNSIGNED NOT NULL,
  `comment_id` bigint(20) UNSIGNED NOT NULL DEFAULT 0,
  `meta_key` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `meta_value` longtext COLLATE utf8mb4_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Table structure for table `wp_comments`
--

CREATE TABLE `wp_comments` (
  `comment_ID` bigint(20) UNSIGNED NOT NULL,
  `comment_post_ID` bigint(20) UNSIGNED NOT NULL DEFAULT 0,
  `comment_author` tinytext COLLATE utf8mb4_unicode_ci NOT NULL,
  `comment_author_email` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `comment_author_url` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `comment_author_IP` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `comment_date` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `comment_date_gmt` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `comment_content` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `comment_karma` int(11) NOT NULL DEFAULT 0,
  `comment_approved` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '1',
  `comment_agent` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `comment_type` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'comment',
  `comment_parent` bigint(20) UNSIGNED NOT NULL DEFAULT 0,
  `user_id` bigint(20) UNSIGNED NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `wp_comments`
--

INSERT INTO `wp_comments` (`comment_ID`, `comment_post_ID`, `comment_author`, `comment_author_email`, `comment_author_url`, `comment_author_IP`, `comment_date`, `comment_date_gmt`, `comment_content`, `comment_karma`, `comment_approved`, `comment_agent`, `comment_type`, `comment_parent`, `user_id`) VALUES
(1, 1, 'A WordPress Commenter', 'wapuu@wordpress.example', 'https://wordpress.org/', '', '2021-04-14 18:38:27', '2021-04-14 18:38:27', 'Hi, this is a comment.\nTo get started with moderating, editing, and deleting comments, please visit the Comments screen in the dashboard.\nCommenter avatars come from <a href=\"https://gravatar.com\">Gravatar</a>.', 0, '1', '', 'comment', 0, 0);

-- --------------------------------------------------------

--
-- Table structure for table `wp_links`
--

CREATE TABLE `wp_links` (
  `link_id` bigint(20) UNSIGNED NOT NULL,
  `link_url` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `link_name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `link_image` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `link_target` varchar(25) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `link_description` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `link_visible` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'Y',
  `link_owner` bigint(20) UNSIGNED NOT NULL DEFAULT 1,
  `link_rating` int(11) NOT NULL DEFAULT 0,
  `link_updated` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `link_rel` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `link_notes` mediumtext COLLATE utf8mb4_unicode_ci NOT NULL,
  `link_rss` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Table structure for table `wp_options`
--

CREATE TABLE `wp_options` (
  `option_id` bigint(20) UNSIGNED NOT NULL,
  `option_name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `option_value` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
  `autoload` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'yes'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `wp_options`
--

INSERT INTO `wp_options` (`option_id`, `option_name`, `option_value`, `autoload`) VALUES
(1, 'siteurl', 'https://banjaraclothing.com', 'yes'),
(2, 'home', 'https://banjaraclothing.com', 'yes'),
(3, 'blogname', 'BanjaraClothing', 'yes'),
(4, 'blogdescription', 'Just another WordPress site', 'yes'),
(5, 'users_can_register', '0', 'yes'),
(6, 'admin_email', 'hingerharsh@gmail.com', 'yes'),
(7, 'start_of_week', '1', 'yes'),
(8, 'use_balanceTags', '0', 'yes'),
(9, 'use_smilies', '1', 'yes'),
(10, 'require_name_email', '1', 'yes'),
(11, 'comments_notify', '1', 'yes'),
(12, 'posts_per_rss', '10', 'yes'),
(13, 'rss_use_excerpt', '0', 'yes'),
(14, 'mailserver_url', 'mail.example.com', 'yes'),
(15, 'mailserver_login', 'login@example.com', 'yes'),
(16, 'mailserver_pass', 'password', 'yes'),
(17, 'mailserver_port', '110', 'yes'),
(18, 'default_category', '1', 'yes'),
(19, 'default_comment_status', 'open', 'yes'),
(20, 'default_ping_status', 'open', 'yes'),
(21, 'default_pingback_flag', '1', 'yes'),
(22, 'posts_per_page', '10', 'yes'),
(23, 'date_format', 'F j, Y', 'yes'),
(24, 'time_format', 'g:i a', 'yes'),
(25, 'links_updated_date_format', 'F j, Y g:i a', 'yes'),
(26, 'comment_moderation', '0', 'yes'),
(27, 'moderation_notify', '1', 'yes'),
(28, 'permalink_structure', '/%year%/%monthnum%/%day%/%postname%/', 'yes'),
(29, 'rewrite_rules', 'a:86:{s:11:\"sitemap.xml\";s:33:\"index.php?aiosp_sitemap_path=root\";s:16:\"(.+)-sitemap.xml\";s:40:\"index.php?aiosp_sitemap_path=$matches[1]\";s:21:\"(.+)-sitemap(\\d+).xml\";s:71:\"index.php?aiosp_sitemap_path=$matches[1]&aiosp_sitemap_page=$matches[2]\";s:11:\"sitemap.rss\";s:32:\"index.php?aiosp_sitemap_path=rss\";s:18:\"sitemap.latest.rss\";s:32:\"index.php?aiosp_sitemap_path=rss\";s:11:\"^wp-json/?$\";s:22:\"index.php?rest_route=/\";s:14:\"^wp-json/(.*)?\";s:33:\"index.php?rest_route=/$matches[1]\";s:21:\"^index.php/wp-json/?$\";s:22:\"index.php?rest_route=/\";s:24:\"^index.php/wp-json/(.*)?\";s:33:\"index.php?rest_route=/$matches[1]\";s:17:\"^wp-sitemap\\.xml$\";s:23:\"index.php?sitemap=index\";s:17:\"^wp-sitemap\\.xsl$\";s:36:\"index.php?sitemap-stylesheet=sitemap\";s:23:\"^wp-sitemap-index\\.xsl$\";s:34:\"index.php?sitemap-stylesheet=index\";s:48:\"^wp-sitemap-([a-z]+?)-([a-z\\d_-]+?)-(\\d+?)\\.xml$\";s:75:\"index.php?sitemap=$matches[1]&sitemap-subtype=$matches[2]&paged=$matches[3]\";s:34:\"^wp-sitemap-([a-z]+?)-(\\d+?)\\.xml$\";s:47:\"index.php?sitemap=$matches[1]&paged=$matches[2]\";s:12:\"robots\\.txt$\";s:18:\"index.php?robots=1\";s:13:\"favicon\\.ico$\";s:19:\"index.php?favicon=1\";s:48:\".*wp-(atom|rdf|rss|rss2|feed|commentsrss2)\\.php$\";s:18:\"index.php?feed=old\";s:20:\".*wp-app\\.php(/.*)?$\";s:19:\"index.php?error=403\";s:18:\".*wp-register.php$\";s:23:\"index.php?register=true\";s:32:\"feed/(feed|rdf|rss|rss2|atom)/?$\";s:27:\"index.php?&feed=$matches[1]\";s:27:\"(feed|rdf|rss|rss2|atom)/?$\";s:27:\"index.php?&feed=$matches[1]\";s:8:\"embed/?$\";s:21:\"index.php?&embed=true\";s:20:\"page/?([0-9]{1,})/?$\";s:28:\"index.php?&paged=$matches[1]\";s:41:\"comments/feed/(feed|rdf|rss|rss2|atom)/?$\";s:42:\"index.php?&feed=$matches[1]&withcomments=1\";s:36:\"comments/(feed|rdf|rss|rss2|atom)/?$\";s:42:\"index.php?&feed=$matches[1]&withcomments=1\";s:17:\"comments/embed/?$\";s:21:\"index.php?&embed=true\";s:44:\"search/(.+)/feed/(feed|rdf|rss|rss2|atom)/?$\";s:40:\"index.php?s=$matches[1]&feed=$matches[2]\";s:39:\"search/(.+)/(feed|rdf|rss|rss2|atom)/?$\";s:40:\"index.php?s=$matches[1]&feed=$matches[2]\";s:20:\"search/(.+)/embed/?$\";s:34:\"index.php?s=$matches[1]&embed=true\";s:32:\"search/(.+)/page/?([0-9]{1,})/?$\";s:41:\"index.php?s=$matches[1]&paged=$matches[2]\";s:14:\"search/(.+)/?$\";s:23:\"index.php?s=$matches[1]\";s:47:\"author/([^/]+)/feed/(feed|rdf|rss|rss2|atom)/?$\";s:50:\"index.php?author_name=$matches[1]&feed=$matches[2]\";s:42:\"author/([^/]+)/(feed|rdf|rss|rss2|atom)/?$\";s:50:\"index.php?author_name=$matches[1]&feed=$matches[2]\";s:23:\"author/([^/]+)/embed/?$\";s:44:\"index.php?author_name=$matches[1]&embed=true\";s:35:\"author/([^/]+)/page/?([0-9]{1,})/?$\";s:51:\"index.php?author_name=$matches[1]&paged=$matches[2]\";s:17:\"author/([^/]+)/?$\";s:33:\"index.php?author_name=$matches[1]\";s:69:\"([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/feed/(feed|rdf|rss|rss2|atom)/?$\";s:80:\"index.php?year=$matches[1]&monthnum=$matches[2]&day=$matches[3]&feed=$matches[4]\";s:64:\"([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/(feed|rdf|rss|rss2|atom)/?$\";s:80:\"index.php?year=$matches[1]&monthnum=$matches[2]&day=$matches[3]&feed=$matches[4]\";s:45:\"([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/embed/?$\";s:74:\"index.php?year=$matches[1]&monthnum=$matches[2]&day=$matches[3]&embed=true\";s:57:\"([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/page/?([0-9]{1,})/?$\";s:81:\"index.php?year=$matches[1]&monthnum=$matches[2]&day=$matches[3]&paged=$matches[4]\";s:39:\"([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/?$\";s:63:\"index.php?year=$matches[1]&monthnum=$matches[2]&day=$matches[3]\";s:56:\"([0-9]{4})/([0-9]{1,2})/feed/(feed|rdf|rss|rss2|atom)/?$\";s:64:\"index.php?year=$matches[1]&monthnum=$matches[2]&feed=$matches[3]\";s:51:\"([0-9]{4})/([0-9]{1,2})/(feed|rdf|rss|rss2|atom)/?$\";s:64:\"index.php?year=$matches[1]&monthnum=$matches[2]&feed=$matches[3]\";s:32:\"([0-9]{4})/([0-9]{1,2})/embed/?$\";s:58:\"index.php?year=$matches[1]&monthnum=$matches[2]&embed=true\";s:44:\"([0-9]{4})/([0-9]{1,2})/page/?([0-9]{1,})/?$\";s:65:\"index.php?year=$matches[1]&monthnum=$matches[2]&paged=$matches[3]\";s:26:\"([0-9]{4})/([0-9]{1,2})/?$\";s:47:\"index.php?year=$matches[1]&monthnum=$matches[2]\";s:43:\"([0-9]{4})/feed/(feed|rdf|rss|rss2|atom)/?$\";s:43:\"index.php?year=$matches[1]&feed=$matches[2]\";s:38:\"([0-9]{4})/(feed|rdf|rss|rss2|atom)/?$\";s:43:\"index.php?year=$matches[1]&feed=$matches[2]\";s:19:\"([0-9]{4})/embed/?$\";s:37:\"index.php?year=$matches[1]&embed=true\";s:31:\"([0-9]{4})/page/?([0-9]{1,})/?$\";s:44:\"index.php?year=$matches[1]&paged=$matches[2]\";s:13:\"([0-9]{4})/?$\";s:26:\"index.php?year=$matches[1]\";s:58:\"[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}/[^/]+/attachment/([^/]+)/?$\";s:32:\"index.php?attachment=$matches[1]\";s:68:\"[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}/[^/]+/attachment/([^/]+)/trackback/?$\";s:37:\"index.php?attachment=$matches[1]&tb=1\";s:88:\"[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}/[^/]+/attachment/([^/]+)/feed/(feed|rdf|rss|rss2|atom)/?$\";s:49:\"index.php?attachment=$matches[1]&feed=$matches[2]\";s:83:\"[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}/[^/]+/attachment/([^/]+)/(feed|rdf|rss|rss2|atom)/?$\";s:49:\"index.php?attachment=$matches[1]&feed=$matches[2]\";s:83:\"[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}/[^/]+/attachment/([^/]+)/comment-page-([0-9]{1,})/?$\";s:50:\"index.php?attachment=$matches[1]&cpage=$matches[2]\";s:64:\"[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}/[^/]+/attachment/([^/]+)/embed/?$\";s:43:\"index.php?attachment=$matches[1]&embed=true\";s:53:\"([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/([^/]+)/embed/?$\";s:91:\"index.php?year=$matches[1]&monthnum=$matches[2]&day=$matches[3]&name=$matches[4]&embed=true\";s:57:\"([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/([^/]+)/trackback/?$\";s:85:\"index.php?year=$matches[1]&monthnum=$matches[2]&day=$matches[3]&name=$matches[4]&tb=1\";s:77:\"([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/([^/]+)/feed/(feed|rdf|rss|rss2|atom)/?$\";s:97:\"index.php?year=$matches[1]&monthnum=$matches[2]&day=$matches[3]&name=$matches[4]&feed=$matches[5]\";s:72:\"([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/([^/]+)/(feed|rdf|rss|rss2|atom)/?$\";s:97:\"index.php?year=$matches[1]&monthnum=$matches[2]&day=$matches[3]&name=$matches[4]&feed=$matches[5]\";s:65:\"([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/([^/]+)/page/?([0-9]{1,})/?$\";s:98:\"index.php?year=$matches[1]&monthnum=$matches[2]&day=$matches[3]&name=$matches[4]&paged=$matches[5]\";s:72:\"([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/([^/]+)/comment-page-([0-9]{1,})/?$\";s:98:\"index.php?year=$matches[1]&monthnum=$matches[2]&day=$matches[3]&name=$matches[4]&cpage=$matches[5]\";s:61:\"([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/([^/]+)(?:/([0-9]+))?/?$\";s:97:\"index.php?year=$matches[1]&monthnum=$matches[2]&day=$matches[3]&name=$matches[4]&page=$matches[5]\";s:47:\"[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}/[^/]+/([^/]+)/?$\";s:32:\"index.php?attachment=$matches[1]\";s:57:\"[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}/[^/]+/([^/]+)/trackback/?$\";s:37:\"index.php?attachment=$matches[1]&tb=1\";s:77:\"[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}/[^/]+/([^/]+)/feed/(feed|rdf|rss|rss2|atom)/?$\";s:49:\"index.php?attachment=$matches[1]&feed=$matches[2]\";s:72:\"[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}/[^/]+/([^/]+)/(feed|rdf|rss|rss2|atom)/?$\";s:49:\"index.php?attachment=$matches[1]&feed=$matches[2]\";s:72:\"[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}/[^/]+/([^/]+)/comment-page-([0-9]{1,})/?$\";s:50:\"index.php?attachment=$matches[1]&cpage=$matches[2]\";s:53:\"[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}/[^/]+/([^/]+)/embed/?$\";s:43:\"index.php?attachment=$matches[1]&embed=true\";s:64:\"([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/comment-page-([0-9]{1,})/?$\";s:81:\"index.php?year=$matches[1]&monthnum=$matches[2]&day=$matches[3]&cpage=$matches[4]\";s:51:\"([0-9]{4})/([0-9]{1,2})/comment-page-([0-9]{1,})/?$\";s:65:\"index.php?year=$matches[1]&monthnum=$matches[2]&cpage=$matches[3]\";s:38:\"([0-9]{4})/comment-page-([0-9]{1,})/?$\";s:44:\"index.php?year=$matches[1]&cpage=$matches[2]\";s:27:\".?.+?/attachment/([^/]+)/?$\";s:32:\"index.php?attachment=$matches[1]\";s:37:\".?.+?/attachment/([^/]+)/trackback/?$\";s:37:\"index.php?attachment=$matches[1]&tb=1\";s:57:\".?.+?/attachment/([^/]+)/feed/(feed|rdf|rss|rss2|atom)/?$\";s:49:\"index.php?attachment=$matches[1]&feed=$matches[2]\";s:52:\".?.+?/attachment/([^/]+)/(feed|rdf|rss|rss2|atom)/?$\";s:49:\"index.php?attachment=$matches[1]&feed=$matches[2]\";s:52:\".?.+?/attachment/([^/]+)/comment-page-([0-9]{1,})/?$\";s:50:\"index.php?attachment=$matches[1]&cpage=$matches[2]\";s:33:\".?.+?/attachment/([^/]+)/embed/?$\";s:43:\"index.php?attachment=$matches[1]&embed=true\";s:16:\"(.?.+?)/embed/?$\";s:41:\"index.php?pagename=$matches[1]&embed=true\";s:20:\"(.?.+?)/trackback/?$\";s:35:\"index.php?pagename=$matches[1]&tb=1\";s:40:\"(.?.+?)/feed/(feed|rdf|rss|rss2|atom)/?$\";s:47:\"index.php?pagename=$matches[1]&feed=$matches[2]\";s:35:\"(.?.+?)/(feed|rdf|rss|rss2|atom)/?$\";s:47:\"index.php?pagename=$matches[1]&feed=$matches[2]\";s:28:\"(.?.+?)/page/?([0-9]{1,})/?$\";s:48:\"index.php?pagename=$matches[1]&paged=$matches[2]\";s:35:\"(.?.+?)/comment-page-([0-9]{1,})/?$\";s:48:\"index.php?pagename=$matches[1]&cpage=$matches[2]\";s:24:\"(.?.+?)(?:/([0-9]+))?/?$\";s:47:\"index.php?pagename=$matches[1]&page=$matches[2]\";}', 'yes'),
(30, 'hack_file', '0', 'yes'),
(31, 'blog_charset', 'UTF-8', 'yes'),
(32, 'moderation_keys', '', 'no'),
(33, 'active_plugins', 'a:6:{i:0;s:43:\"all-in-one-seo-pack/all_in_one_seo_pack.php\";i:1;s:51:\"all-in-one-wp-migration/all-in-one-wp-migration.php\";i:2;s:50:\"google-analytics-for-wordpress/googleanalytics.php\";i:3;s:35:\"litespeed-cache/litespeed-cache.php\";i:4;s:37:\"optinmonster/optin-monster-wp-api.php\";i:5;s:24:\"wpforms-lite/wpforms.php\";}', 'yes'),
(34, 'category_base', '', 'yes'),
(35, 'ping_sites', 'http://rpc.pingomatic.com/', 'yes'),
(36, 'comment_max_links', '2', 'yes'),
(37, 'gmt_offset', '0', 'yes'),
(38, 'default_email_category', '1', 'yes'),
(39, 'recently_edited', '', 'no'),
(40, 'template', 'twentytwentyone', 'yes'),
(41, 'stylesheet', 'twentytwentyone', 'yes'),
(42, 'comment_registration', '0', 'yes'),
(43, 'html_type', 'text/html', 'yes'),
(44, 'use_trackback', '0', 'yes'),
(45, 'default_role', 'subscriber', 'yes'),
(46, 'db_version', '49752', 'yes'),
(47, 'uploads_use_yearmonth_folders', '1', 'yes'),
(48, 'upload_path', '', 'yes'),
(49, 'blog_public', '1', 'yes'),
(50, 'default_link_category', '2', 'yes'),
(51, 'show_on_front', 'posts', 'yes'),
(52, 'tag_base', '', 'yes'),
(53, 'show_avatars', '1', 'yes'),
(54, 'avatar_rating', 'G', 'yes'),
(55, 'upload_url_path', '', 'yes'),
(56, 'thumbnail_size_w', '150', 'yes'),
(57, 'thumbnail_size_h', '150', 'yes'),
(58, 'thumbnail_crop', '1', 'yes'),
(59, 'medium_size_w', '300', 'yes'),
(60, 'medium_size_h', '300', 'yes'),
(61, 'avatar_default', 'mystery', 'yes'),
(62, 'large_size_w', '1024', 'yes'),
(63, 'large_size_h', '1024', 'yes'),
(64, 'image_default_link_type', 'none', 'yes'),
(65, 'image_default_size', '', 'yes'),
(66, 'image_default_align', '', 'yes'),
(67, 'close_comments_for_old_posts', '0', 'yes'),
(68, 'close_comments_days_old', '14', 'yes'),
(69, 'thread_comments', '1', 'yes'),
(70, 'thread_comments_depth', '5', 'yes'),
(71, 'page_comments', '0', 'yes'),
(72, 'comments_per_page', '50', 'yes'),
(73, 'default_comments_page', 'newest', 'yes'),
(74, 'comment_order', 'asc', 'yes'),
(75, 'sticky_posts', 'a:0:{}', 'yes'),
(76, 'widget_categories', 'a:2:{i:2;a:4:{s:5:\"title\";s:0:\"\";s:5:\"count\";i:0;s:12:\"hierarchical\";i:0;s:8:\"dropdown\";i:0;}s:12:\"_multiwidget\";i:1;}', 'yes'),
(77, 'widget_text', 'a:2:{i:1;a:0:{}s:12:\"_multiwidget\";i:1;}', 'yes'),
(78, 'widget_rss', 'a:2:{i:1;a:0:{}s:12:\"_multiwidget\";i:1;}', 'yes'),
(79, 'uninstall_plugins', 'a:3:{s:35:\"litespeed-cache/litespeed-cache.php\";s:47:\"LiteSpeed\\Activation::uninstall_litespeed_cache\";s:37:\"optinmonster/optin-monster-wp-api.php\";s:32:\"optin_monster_api_uninstall_hook\";s:50:\"google-analytics-for-wordpress/googleanalytics.php\";s:35:\"monsterinsights_lite_uninstall_hook\";}', 'no'),
(80, 'timezone_string', '', 'yes'),
(81, 'page_for_posts', '0', 'yes'),
(82, 'page_on_front', '0', 'yes'),
(83, 'default_post_format', '0', 'yes'),
(84, 'link_manager_enabled', '0', 'yes'),
(85, 'finished_splitting_shared_terms', '1', 'yes'),
(86, 'site_icon', '0', 'yes'),
(87, 'medium_large_size_w', '768', 'yes'),
(88, 'medium_large_size_h', '0', 'yes'),
(89, 'wp_page_for_privacy_policy', '3', 'yes'),
(90, 'show_comments_cookies_opt_in', '1', 'yes'),
(91, 'admin_email_lifespan', '1633977507', 'yes'),
(92, 'disallowed_keys', '', 'no'),
(93, 'comment_previously_approved', '1', 'yes'),
(94, 'auto_plugin_theme_update_emails', 'a:0:{}', 'no'),
(95, 'auto_update_core_dev', 'enabled', 'yes'),
(96, 'auto_update_core_minor', 'enabled', 'yes'),
(97, 'auto_update_core_major', 'enabled', 'yes'),
(98, 'initial_db_version', '49752', 'yes'),
(99, 'wp_user_roles', 'a:5:{s:13:\"administrator\";a:2:{s:4:\"name\";s:13:\"Administrator\";s:12:\"capabilities\";a:81:{s:13:\"switch_themes\";b:1;s:11:\"edit_themes\";b:1;s:16:\"activate_plugins\";b:1;s:12:\"edit_plugins\";b:1;s:10:\"edit_users\";b:1;s:10:\"edit_files\";b:1;s:14:\"manage_options\";b:1;s:17:\"moderate_comments\";b:1;s:17:\"manage_categories\";b:1;s:12:\"manage_links\";b:1;s:12:\"upload_files\";b:1;s:6:\"import\";b:1;s:15:\"unfiltered_html\";b:1;s:10:\"edit_posts\";b:1;s:17:\"edit_others_posts\";b:1;s:20:\"edit_published_posts\";b:1;s:13:\"publish_posts\";b:1;s:10:\"edit_pages\";b:1;s:4:\"read\";b:1;s:8:\"level_10\";b:1;s:7:\"level_9\";b:1;s:7:\"level_8\";b:1;s:7:\"level_7\";b:1;s:7:\"level_6\";b:1;s:7:\"level_5\";b:1;s:7:\"level_4\";b:1;s:7:\"level_3\";b:1;s:7:\"level_2\";b:1;s:7:\"level_1\";b:1;s:7:\"level_0\";b:1;s:17:\"edit_others_pages\";b:1;s:20:\"edit_published_pages\";b:1;s:13:\"publish_pages\";b:1;s:12:\"delete_pages\";b:1;s:19:\"delete_others_pages\";b:1;s:22:\"delete_published_pages\";b:1;s:12:\"delete_posts\";b:1;s:19:\"delete_others_posts\";b:1;s:22:\"delete_published_posts\";b:1;s:20:\"delete_private_posts\";b:1;s:18:\"edit_private_posts\";b:1;s:18:\"read_private_posts\";b:1;s:20:\"delete_private_pages\";b:1;s:18:\"edit_private_pages\";b:1;s:18:\"read_private_pages\";b:1;s:12:\"delete_users\";b:1;s:12:\"create_users\";b:1;s:17:\"unfiltered_upload\";b:1;s:14:\"edit_dashboard\";b:1;s:14:\"update_plugins\";b:1;s:14:\"delete_plugins\";b:1;s:15:\"install_plugins\";b:1;s:13:\"update_themes\";b:1;s:14:\"install_themes\";b:1;s:11:\"update_core\";b:1;s:10:\"list_users\";b:1;s:12:\"remove_users\";b:1;s:13:\"promote_users\";b:1;s:18:\"edit_theme_options\";b:1;s:13:\"delete_themes\";b:1;s:6:\"export\";b:1;s:17:\"aioseo_manage_seo\";b:1;s:19:\"aioseo_setup_wizard\";b:1;s:23:\"aioseo_general_settings\";b:1;s:33:\"aioseo_search_appearance_settings\";b:1;s:31:\"aioseo_social_networks_settings\";b:1;s:23:\"aioseo_sitemap_settings\";b:1;s:30:\"aioseo_internal_links_settings\";b:1;s:25:\"aioseo_redirects_settings\";b:1;s:28:\"aioseo_seo_analysis_settings\";b:1;s:21:\"aioseo_tools_settings\";b:1;s:31:\"aioseo_feature_manager_settings\";b:1;s:20:\"aioseo_page_analysis\";b:1;s:28:\"aioseo_page_general_settings\";b:1;s:29:\"aioseo_page_advanced_settings\";b:1;s:27:\"aioseo_page_schema_settings\";b:1;s:27:\"aioseo_page_social_settings\";b:1;s:25:\"aioseo_local_seo_settings\";b:1;s:30:\"aioseo_page_local_seo_settings\";b:1;s:20:\"aioseo_about_us_page\";b:1;s:12:\"aioseo_admin\";b:1;}}s:6:\"editor\";a:2:{s:4:\"name\";s:6:\"Editor\";s:12:\"capabilities\";a:40:{s:17:\"moderate_comments\";b:1;s:17:\"manage_categories\";b:1;s:12:\"manage_links\";b:1;s:12:\"upload_files\";b:1;s:15:\"unfiltered_html\";b:1;s:10:\"edit_posts\";b:1;s:17:\"edit_others_posts\";b:1;s:20:\"edit_published_posts\";b:1;s:13:\"publish_posts\";b:1;s:10:\"edit_pages\";b:1;s:4:\"read\";b:1;s:7:\"level_7\";b:1;s:7:\"level_6\";b:1;s:7:\"level_5\";b:1;s:7:\"level_4\";b:1;s:7:\"level_3\";b:1;s:7:\"level_2\";b:1;s:7:\"level_1\";b:1;s:7:\"level_0\";b:1;s:17:\"edit_others_pages\";b:1;s:20:\"edit_published_pages\";b:1;s:13:\"publish_pages\";b:1;s:12:\"delete_pages\";b:1;s:19:\"delete_others_pages\";b:1;s:22:\"delete_published_pages\";b:1;s:12:\"delete_posts\";b:1;s:19:\"delete_others_posts\";b:1;s:22:\"delete_published_posts\";b:1;s:20:\"delete_private_posts\";b:1;s:18:\"edit_private_posts\";b:1;s:18:\"read_private_posts\";b:1;s:20:\"delete_private_pages\";b:1;s:18:\"edit_private_pages\";b:1;s:18:\"read_private_pages\";b:1;s:20:\"aioseo_page_analysis\";b:1;s:28:\"aioseo_page_general_settings\";b:1;s:29:\"aioseo_page_advanced_settings\";b:1;s:27:\"aioseo_page_schema_settings\";b:1;s:27:\"aioseo_page_social_settings\";b:1;s:30:\"aioseo_page_local_seo_settings\";b:1;}}s:6:\"author\";a:2:{s:4:\"name\";s:6:\"Author\";s:12:\"capabilities\";a:16:{s:12:\"upload_files\";b:1;s:10:\"edit_posts\";b:1;s:20:\"edit_published_posts\";b:1;s:13:\"publish_posts\";b:1;s:4:\"read\";b:1;s:7:\"level_2\";b:1;s:7:\"level_1\";b:1;s:7:\"level_0\";b:1;s:12:\"delete_posts\";b:1;s:22:\"delete_published_posts\";b:1;s:20:\"aioseo_page_analysis\";b:1;s:28:\"aioseo_page_general_settings\";b:1;s:29:\"aioseo_page_advanced_settings\";b:1;s:27:\"aioseo_page_schema_settings\";b:1;s:27:\"aioseo_page_social_settings\";b:1;s:30:\"aioseo_page_local_seo_settings\";b:1;}}s:11:\"contributor\";a:2:{s:4:\"name\";s:11:\"Contributor\";s:12:\"capabilities\";a:5:{s:10:\"edit_posts\";b:1;s:4:\"read\";b:1;s:7:\"level_1\";b:1;s:7:\"level_0\";b:1;s:12:\"delete_posts\";b:1;}}s:10:\"subscriber\";a:2:{s:4:\"name\";s:10:\"Subscriber\";s:12:\"capabilities\";a:2:{s:4:\"read\";b:1;s:7:\"level_0\";b:1;}}}', 'yes'),
(100, 'fresh_site', '1', 'yes'),
(101, 'widget_search', 'a:2:{i:2;a:1:{s:5:\"title\";s:0:\"\";}s:12:\"_multiwidget\";i:1;}', 'yes'),
(102, 'widget_recent-posts', 'a:2:{i:2;a:2:{s:5:\"title\";s:0:\"\";s:6:\"number\";i:5;}s:12:\"_multiwidget\";i:1;}', 'yes'),
(103, 'widget_recent-comments', 'a:2:{i:2;a:2:{s:5:\"title\";s:0:\"\";s:6:\"number\";i:5;}s:12:\"_multiwidget\";i:1;}', 'yes'),
(104, 'widget_archives', 'a:2:{i:2;a:3:{s:5:\"title\";s:0:\"\";s:5:\"count\";i:0;s:8:\"dropdown\";i:0;}s:12:\"_multiwidget\";i:1;}', 'yes'),
(105, 'widget_meta', 'a:2:{i:2;a:1:{s:5:\"title\";s:0:\"\";}s:12:\"_multiwidget\";i:1;}', 'yes'),
(106, 'sidebars_widgets', 'a:4:{s:19:\"wp_inactive_widgets\";a:0:{}s:9:\"sidebar-1\";a:3:{i:0;s:8:\"search-2\";i:1;s:14:\"recent-posts-2\";i:2;s:17:\"recent-comments-2\";}s:9:\"sidebar-2\";a:3:{i:0;s:10:\"archives-2\";i:1;s:12:\"categories-2\";i:2;s:6:\"meta-2\";}s:13:\"array_version\";i:3;}', 'yes'),
(107, 'cron', 'a:9:{i:1619690019;a:1:{s:26:\"action_scheduler_run_queue\";a:1:{s:32:\"0d04ed39571b55704c122d726248bbac\";a:3:{s:8:\"schedule\";s:12:\"every_minute\";s:4:\"args\";a:1:{i:0;s:7:\"WP Cron\";}s:8:\"interval\";i:60;}}}i:1619690020;a:3:{s:27:\"litespeed_task_imgoptm_pull\";a:1:{s:32:\"40cd750bba9870f18aada2478b24840a\";a:3:{s:8:\"schedule\";s:16:\"litespeed_filter\";s:4:\"args\";a:0:{}s:8:\"interval\";i:60;}}s:19:\"litespeed_task_ccss\";a:1:{s:32:\"40cd750bba9870f18aada2478b24840a\";a:3:{s:8:\"schedule\";s:16:\"litespeed_filter\";s:4:\"args\";a:0:{}s:8:\"interval\";i:60;}}s:19:\"litespeed_task_lqip\";a:1:{s:32:\"40cd750bba9870f18aada2478b24840a\";a:3:{s:8:\"schedule\";s:16:\"litespeed_filter\";s:4:\"args\";a:0:{}s:8:\"interval\";i:60;}}}i:1619692708;a:1:{s:34:\"wp_privacy_delete_old_export_files\";a:1:{s:32:\"40cd750bba9870f18aada2478b24840a\";a:3:{s:8:\"schedule\";s:6:\"hourly\";s:4:\"args\";a:0:{}s:8:\"interval\";i:3600;}}}i:1619721508;a:6:{s:30:\"wp_site_health_scheduled_check\";a:1:{s:32:\"40cd750bba9870f18aada2478b24840a\";a:3:{s:8:\"schedule\";s:6:\"weekly\";s:4:\"args\";a:0:{}s:8:\"interval\";i:604800;}}s:32:\"recovery_mode_clean_expired_keys\";a:1:{s:32:\"40cd750bba9870f18aada2478b24840a\";a:3:{s:8:\"schedule\";s:5:\"daily\";s:4:\"args\";a:0:{}s:8:\"interval\";i:86400;}}s:18:\"wp_https_detection\";a:1:{s:32:\"40cd750bba9870f18aada2478b24840a\";a:3:{s:8:\"schedule\";s:10:\"twicedaily\";s:4:\"args\";a:0:{}s:8:\"interval\";i:43200;}}s:16:\"wp_version_check\";a:1:{s:32:\"40cd750bba9870f18aada2478b24840a\";a:3:{s:8:\"schedule\";s:10:\"twicedaily\";s:4:\"args\";a:0:{}s:8:\"interval\";i:43200;}}s:17:\"wp_update_plugins\";a:1:{s:32:\"40cd750bba9870f18aada2478b24840a\";a:3:{s:8:\"schedule\";s:10:\"twicedaily\";s:4:\"args\";a:0:{}s:8:\"interval\";i:43200;}}s:16:\"wp_update_themes\";a:1:{s:32:\"40cd750bba9870f18aada2478b24840a\";a:3:{s:8:\"schedule\";s:10:\"twicedaily\";s:4:\"args\";a:0:{}s:8:\"interval\";i:43200;}}}i:1619721674;a:3:{s:21:\"ai1wm_storage_cleanup\";a:1:{s:32:\"40cd750bba9870f18aada2478b24840a\";a:3:{s:8:\"schedule\";s:5:\"daily\";s:4:\"args\";a:0:{}s:8:\"interval\";i:86400;}}s:19:\"wp_scheduled_delete\";a:1:{s:32:\"40cd750bba9870f18aada2478b24840a\";a:3:{s:8:\"schedule\";s:5:\"daily\";s:4:\"args\";a:0:{}s:8:\"interval\";i:86400;}}s:25:\"delete_expired_transients\";a:1:{s:32:\"40cd750bba9870f18aada2478b24840a\";a:3:{s:8:\"schedule\";s:5:\"daily\";s:4:\"args\";a:0:{}s:8:\"interval\";i:86400;}}}i:1619722241;a:1:{s:30:\"wp_scheduled_auto_draft_delete\";a:1:{s:32:\"40cd750bba9870f18aada2478b24840a\";a:3:{s:8:\"schedule\";s:5:\"daily\";s:4:\"args\";a:0:{}s:8:\"interval\";i:86400;}}}i:1619907069;a:1:{s:35:\"monsterinsights_usage_tracking_cron\";a:1:{s:32:\"40cd750bba9870f18aada2478b24840a\";a:3:{s:8:\"schedule\";s:6:\"weekly\";s:4:\"args\";a:0:{}s:8:\"interval\";i:604800;}}}i:1620050400;a:1:{s:28:\"wpforms_email_summaries_cron\";a:1:{s:32:\"40cd750bba9870f18aada2478b24840a\";a:3:{s:8:\"schedule\";s:30:\"wpforms_email_summaries_weekly\";s:4:\"args\";a:0:{}s:8:\"interval\";i:604800;}}}s:7:\"version\";i:2;}', 'yes'),
(108, 'widget_pages', 'a:1:{s:12:\"_multiwidget\";i:1;}', 'yes'),
(109, 'widget_calendar', 'a:1:{s:12:\"_multiwidget\";i:1;}', 'yes'),
(110, 'widget_media_audio', 'a:1:{s:12:\"_multiwidget\";i:1;}', 'yes'),
(111, 'widget_media_image', 'a:1:{s:12:\"_multiwidget\";i:1;}', 'yes'),
(112, 'widget_media_gallery', 'a:1:{s:12:\"_multiwidget\";i:1;}', 'yes'),
(113, 'widget_media_video', 'a:1:{s:12:\"_multiwidget\";i:1;}', 'yes'),
(114, 'widget_tag_cloud', 'a:1:{s:12:\"_multiwidget\";i:1;}', 'yes'),
(115, 'widget_nav_menu', 'a:1:{s:12:\"_multiwidget\";i:1;}', 'yes'),
(116, 'widget_custom_html', 'a:1:{s:12:\"_multiwidget\";i:1;}', 'yes'),
(118, 'recovery_keys', 'a:0:{}', 'yes'),
(119, 'theme_mods_twentytwentyone', 'a:1:{s:18:\"custom_css_post_id\";i:-1;}', 'yes'),
(120, 'https_detection_errors', 'a:0:{}', 'yes'),
(131, 'litespeed.cloud._summary', 'a:2:{s:19:\"curr_request.wp/ver\";i:0;s:19:\"last_request.wp/ver\";i:1618425520;}', 'yes'),
(132, 'litespeed.conf._version', '3.6.4', 'yes'),
(133, 'litespeed.conf.hash', 'ZT2tOjfnk7lw3glPyPEwx6Josg0mrZiy', 'yes'),
(134, 'litespeed.conf.auto_upgrade', '', 'yes'),
(135, 'litespeed.conf.api_key', '', 'yes'),
(136, 'litespeed.conf.server_ip', '', 'yes'),
(137, 'litespeed.conf.news', '1', 'yes'),
(138, 'litespeed.conf.cache', '1', 'yes'),
(139, 'litespeed.conf.cache-priv', '1', 'yes'),
(140, 'litespeed.conf.cache-commenter', '1', 'yes'),
(141, 'litespeed.conf.cache-rest', '1', 'yes'),
(142, 'litespeed.conf.cache-page_login', '1', 'yes'),
(143, 'litespeed.conf.cache-favicon', '1', 'yes'),
(144, 'litespeed.conf.cache-resources', '1', 'yes'),
(145, 'litespeed.conf.cache-mobile', '', 'yes'),
(146, 'litespeed.conf.cache-mobile_rules', 'a:7:{i:0;s:6:\"Mobile\";i:1;s:7:\"Android\";i:2;s:5:\"Silk/\";i:3;s:6:\"Kindle\";i:4;s:10:\"BlackBerry\";i:5;s:10:\"Opera Mini\";i:6;s:10:\"Opera Mobi\";}', 'yes'),
(147, 'litespeed.conf.cache-browser', '', 'yes'),
(148, 'litespeed.conf.cache-exc_useragents', 'a:0:{}', 'yes'),
(149, 'litespeed.conf.cache-exc_cookies', 'a:0:{}', 'yes'),
(150, 'litespeed.conf.cache-exc_qs', 'a:0:{}', 'yes'),
(151, 'litespeed.conf.cache-exc_cat', 'a:0:{}', 'yes'),
(152, 'litespeed.conf.cache-exc_tag', 'a:0:{}', 'yes'),
(153, 'litespeed.conf.cache-force_uri', 'a:0:{}', 'yes'),
(154, 'litespeed.conf.cache-force_pub_uri', 'a:0:{}', 'yes'),
(155, 'litespeed.conf.cache-priv_uri', 'a:0:{}', 'yes'),
(156, 'litespeed.conf.cache-exc', 'a:0:{}', 'yes'),
(157, 'litespeed.conf.cache-exc_roles', 'a:0:{}', 'yes'),
(158, 'litespeed.conf.cache-drop_qs', 'a:4:{i:0;s:6:\"fbclid\";i:1;s:5:\"gclid\";i:2;s:4:\"utm*\";i:3;s:3:\"_ga\";}', 'yes'),
(159, 'litespeed.conf.cache-ttl_pub', '604800', 'yes'),
(160, 'litespeed.conf.cache-ttl_priv', '1800', 'yes'),
(161, 'litespeed.conf.cache-ttl_frontpage', '604800', 'yes'),
(162, 'litespeed.conf.cache-ttl_feed', '604800', 'yes'),
(163, 'litespeed.conf.cache-ttl_rest', '604800', 'yes'),
(164, 'litespeed.conf.cache-ttl_browser', '31557600', 'yes'),
(165, 'litespeed.conf.cache-ttl_status', 'a:3:{i:0;s:8:\"403 3600\";i:1;s:8:\"404 3600\";i:2;s:8:\"500 3600\";}', 'yes'),
(166, 'litespeed.conf.cache-login_cookie', '', 'yes'),
(167, 'litespeed.conf.cache-vary_group', 'a:0:{}', 'yes'),
(168, 'litespeed.conf.purge-upgrade', '1', 'yes'),
(169, 'litespeed.conf.purge-stale', '', 'yes'),
(170, 'litespeed.conf.purge-post_all', '', 'yes'),
(171, 'litespeed.conf.purge-post_f', '1', 'yes'),
(172, 'litespeed.conf.purge-post_h', '1', 'yes'),
(173, 'litespeed.conf.purge-post_p', '1', 'yes'),
(174, 'litespeed.conf.purge-post_pwrp', '1', 'yes'),
(175, 'litespeed.conf.purge-post_a', '1', 'yes'),
(176, 'litespeed.conf.purge-post_y', '', 'yes'),
(177, 'litespeed.conf.purge-post_m', '1', 'yes'),
(178, 'litespeed.conf.purge-post_d', '', 'yes'),
(179, 'litespeed.conf.purge-post_t', '1', 'yes'),
(180, 'litespeed.conf.purge-post_pt', '1', 'yes'),
(181, 'litespeed.conf.purge-timed_urls', 'a:0:{}', 'yes'),
(182, 'litespeed.conf.purge-timed_urls_time', '', 'yes'),
(183, 'litespeed.conf.purge-hook_all', 'a:10:{i:0;s:12:\"switch_theme\";i:1;s:18:\"wp_create_nav_menu\";i:2;s:18:\"wp_update_nav_menu\";i:3;s:18:\"wp_delete_nav_menu\";i:4;s:11:\"create_term\";i:5;s:10:\"edit_terms\";i:6;s:11:\"delete_term\";i:7;s:8:\"add_link\";i:8;s:9:\"edit_link\";i:9;s:11:\"delete_link\";}', 'yes'),
(184, 'litespeed.conf.esi', '', 'yes'),
(185, 'litespeed.conf.esi-cache_admbar', '1', 'yes'),
(186, 'litespeed.conf.esi-cache_commform', '1', 'yes'),
(187, 'litespeed.conf.esi-nonce', 'a:2:{i:0;s:11:\"stats_nonce\";i:1;s:15:\"subscribe_nonce\";}', 'yes'),
(188, 'litespeed.conf.util-instant_click', '', 'yes'),
(189, 'litespeed.conf.util-no_https_vary', '', 'yes'),
(190, 'litespeed.conf.debug-disable_all', '', 'yes'),
(191, 'litespeed.conf.debug', '', 'yes'),
(192, 'litespeed.conf.debug-ips', 'a:1:{i:0;s:9:\"127.0.0.1\";}', 'yes'),
(193, 'litespeed.conf.debug-level', '', 'yes'),
(194, 'litespeed.conf.debug-filesize', '3', 'yes'),
(195, 'litespeed.conf.debug-cookie', '', 'yes'),
(196, 'litespeed.conf.debug-collaps_qs', '', 'yes'),
(197, 'litespeed.conf.debug-inc', 'a:0:{}', 'yes'),
(198, 'litespeed.conf.debug-exc', 'a:0:{}', 'yes'),
(199, 'litespeed.conf.db_optm-revisions_max', '0', 'yes'),
(200, 'litespeed.conf.db_optm-revisions_age', '0', 'yes'),
(201, 'litespeed.conf.optm-css_min', '', 'yes'),
(202, 'litespeed.conf.optm-css_comb', '', 'yes'),
(203, 'litespeed.conf.optm-css_comb_ext_inl', '', 'yes'),
(204, 'litespeed.conf.optm-ucss', '', 'yes'),
(205, 'litespeed.conf.optm-ucss_async', '', 'yes'),
(206, 'litespeed.conf.optm-css_http2', '', 'yes'),
(207, 'litespeed.conf.optm-css_exc', 'a:0:{}', 'yes'),
(208, 'litespeed.conf.optm-js_min', '', 'yes'),
(209, 'litespeed.conf.optm-js_comb', '', 'yes'),
(210, 'litespeed.conf.optm-js_comb_ext_inl', '', 'yes'),
(211, 'litespeed.conf.optm-js_http2', '', 'yes'),
(212, 'litespeed.conf.optm-js_exc', 'a:2:{i:0;s:9:\"jquery.js\";i:1;s:13:\"jquery.min.js\";}', 'yes'),
(213, 'litespeed.conf.optm-ttl', '604800', 'yes'),
(214, 'litespeed.conf.optm-html_min', '', 'yes'),
(215, 'litespeed.conf.optm-qs_rm', '', 'yes'),
(216, 'litespeed.conf.optm-ggfonts_rm', '', 'yes'),
(217, 'litespeed.conf.optm-css_async', '', 'yes'),
(218, 'litespeed.conf.optm-ccss_gen', '1', 'yes'),
(219, 'litespeed.conf.optm-ccss_async', '1', 'yes'),
(220, 'litespeed.conf.optm-css_async_inline', '1', 'yes'),
(221, 'litespeed.conf.optm-css_font_display', '', 'yes'),
(222, 'litespeed.conf.optm-js_defer', '', 'yes'),
(223, 'litespeed.conf.optm-js_inline_defer', '', 'yes'),
(224, 'litespeed.conf.optm-emoji_rm', '', 'yes'),
(225, 'litespeed.conf.optm-noscript_rm', '', 'yes'),
(226, 'litespeed.conf.optm-ggfonts_async', '', 'yes'),
(227, 'litespeed.conf.optm-exc_roles', 'a:0:{}', 'yes'),
(228, 'litespeed.conf.optm-ccss_con', '', 'yes'),
(229, 'litespeed.conf.optm-js_defer_exc', 'a:2:{i:0;s:9:\"jquery.js\";i:1;s:13:\"jquery.min.js\";}', 'yes'),
(230, 'litespeed.conf.optm-dns_prefetch', 'a:0:{}', 'yes'),
(231, 'litespeed.conf.optm-dns_prefetch_ctrl', '', 'yes'),
(232, 'litespeed.conf.optm-exc', 'a:0:{}', 'yes'),
(233, 'litespeed.conf.optm-ccss_sep_posttype', 'a:0:{}', 'yes'),
(234, 'litespeed.conf.optm-ccss_sep_uri', 'a:0:{}', 'yes'),
(235, 'litespeed.conf.object', '', 'yes'),
(236, 'litespeed.conf.object-kind', '', 'yes'),
(237, 'litespeed.conf.object-host', 'localhost', 'yes'),
(238, 'litespeed.conf.object-port', '11211', 'yes'),
(239, 'litespeed.conf.object-life', '360', 'yes'),
(240, 'litespeed.conf.object-persistent', '1', 'yes'),
(241, 'litespeed.conf.object-admin', '1', 'yes'),
(242, 'litespeed.conf.object-transients', '1', 'yes'),
(243, 'litespeed.conf.object-db_id', '0', 'yes'),
(244, 'litespeed.conf.object-user', '', 'yes'),
(245, 'litespeed.conf.object-pswd', '', 'yes'),
(246, 'litespeed.conf.object-global_groups', 'a:12:{i:0;s:5:\"users\";i:1;s:10:\"userlogins\";i:2;s:8:\"usermeta\";i:3;s:9:\"user_meta\";i:4;s:14:\"site-transient\";i:5;s:12:\"site-options\";i:6;s:11:\"site-lookup\";i:7;s:11:\"blog-lookup\";i:8;s:12:\"blog-details\";i:9;s:3:\"rss\";i:10;s:12:\"global-posts\";i:11;s:13:\"blog-id-cache\";}', 'yes'),
(247, 'litespeed.conf.object-non_persistent_groups', 'a:4:{i:0;s:7:\"comment\";i:1;s:6:\"counts\";i:2;s:7:\"plugins\";i:3;s:13:\"wc_session_id\";}', 'yes'),
(248, 'litespeed.conf.discuss-avatar_cache', '', 'yes'),
(249, 'litespeed.conf.discuss-avatar_cron', '', 'yes'),
(250, 'litespeed.conf.discuss-avatar_cache_ttl', '604800', 'yes'),
(251, 'litespeed.conf.optm-localize', '', 'yes'),
(252, 'litespeed.conf.optm-localize_domains', 'a:7:{i:0;s:23:\"### Popular scripts ###\";i:1;s:39:\"https://platform.twitter.com/widgets.js\";i:2;s:39:\"https://www.google.com/recaptcha/api.js\";i:3;s:45:\"https://www.google-analytics.com/analytics.js\";i:4;s:39:\"https://www.googletagmanager.com/gtm.js\";i:5;s:47:\"https://www.googletagservices.com/tag/js/gpt.js\";i:6;s:46:\"https://connect.facebook.net/en_US/fbevents.js\";}', 'yes'),
(253, 'litespeed.conf.media-lazy', '', 'yes'),
(254, 'litespeed.conf.media-lazy_placeholder', '', 'yes'),
(255, 'litespeed.conf.media-placeholder_resp', '', 'yes'),
(256, 'litespeed.conf.media-placeholder_resp_color', '#cfd4db', 'yes'),
(257, 'litespeed.conf.media-placeholder_resp_svg', '<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{width}\" height=\"{height}\" viewBox=\"0 0 {width} {height}\"><rect width=\"100%\" height=\"100%\" fill=\"{color}\"/></svg>', 'yes'),
(258, 'litespeed.conf.media-lqip', '', 'yes'),
(259, 'litespeed.conf.media-lqip_qual', '4', 'yes'),
(260, 'litespeed.conf.media-lqip_min_w', '150', 'yes'),
(261, 'litespeed.conf.media-lqip_min_h', '150', 'yes'),
(262, 'litespeed.conf.media-placeholder_resp_async', '1', 'yes'),
(263, 'litespeed.conf.media-iframe_lazy', '', 'yes'),
(264, 'litespeed.conf.media-lazyjs_inline', '', 'yes'),
(265, 'litespeed.conf.media-lazy_exc', 'a:0:{}', 'yes'),
(266, 'litespeed.conf.media-lazy_cls_exc', 'a:1:{i:0;s:15:\"wmu-preview-img\";}', 'yes'),
(267, 'litespeed.conf.media-lazy_parent_cls_exc', 'a:0:{}', 'yes'),
(268, 'litespeed.conf.media-iframe_lazy_cls_exc', 'a:0:{}', 'yes'),
(269, 'litespeed.conf.media-iframe_lazy_parent_cls_exc', 'a:0:{}', 'yes'),
(270, 'litespeed.conf.media-lazy_uri_exc', 'a:0:{}', 'yes'),
(271, 'litespeed.conf.media-lqip_exc', 'a:0:{}', 'yes'),
(272, 'litespeed.conf.img_optm-auto', '', 'yes'),
(273, 'litespeed.conf.img_optm-cron', '1', 'yes'),
(274, 'litespeed.conf.img_optm-ori', '1', 'yes'),
(275, 'litespeed.conf.img_optm-rm_bkup', '', 'yes'),
(276, 'litespeed.conf.img_optm-webp', '', 'yes'),
(277, 'litespeed.conf.img_optm-lossless', '', 'yes'),
(278, 'litespeed.conf.img_optm-exif', '', 'yes'),
(279, 'litespeed.conf.img_optm-webp_replace', '', 'yes'),
(280, 'litespeed.conf.img_optm-webp_attr', 'a:7:{i:0;s:7:\"img.src\";i:1;s:14:\"div.data-thumb\";i:2;s:12:\"img.data-src\";i:3;s:20:\"div.data-large_image\";i:4;s:19:\"img.retina_logo_url\";i:5;s:23:\"div.data-parallax-image\";i:6;s:12:\"video.poster\";}', 'yes'),
(281, 'litespeed.conf.img_optm-webp_replace_srcset', '', 'yes'),
(282, 'litespeed.conf.img_optm-jpg_quality', '82', 'yes'),
(283, 'litespeed.conf.crawler', '', 'yes'),
(284, 'litespeed.conf.crawler-usleep', '500', 'yes'),
(285, 'litespeed.conf.crawler-run_duration', '400', 'yes'),
(286, 'litespeed.conf.crawler-run_interval', '600', 'yes'),
(287, 'litespeed.conf.crawler-crawl_interval', '302400', 'yes'),
(288, 'litespeed.conf.crawler-threads', '3', 'yes'),
(289, 'litespeed.conf.crawler-timeout', '30', 'yes'),
(290, 'litespeed.conf.crawler-load_limit', '1', 'yes'),
(291, 'litespeed.conf.crawler-sitemap', '', 'yes'),
(292, 'litespeed.conf.crawler-drop_domain', '1', 'yes'),
(293, 'litespeed.conf.crawler-map_timeout', '120', 'yes'),
(294, 'litespeed.conf.crawler-roles', 'a:0:{}', 'yes'),
(295, 'litespeed.conf.crawler-cookies', 'a:0:{}', 'yes'),
(296, 'litespeed.conf.misc-htaccess_front', '', 'yes'),
(297, 'litespeed.conf.misc-htaccess_back', '', 'yes'),
(298, 'litespeed.conf.misc-heartbeat_front', '', 'yes'),
(299, 'litespeed.conf.misc-heartbeat_front_ttl', '60', 'yes'),
(300, 'litespeed.conf.misc-heartbeat_back', '', 'yes'),
(301, 'litespeed.conf.misc-heartbeat_back_ttl', '60', 'yes'),
(302, 'litespeed.conf.misc-heartbeat_editor', '', 'yes'),
(303, 'litespeed.conf.misc-heartbeat_editor_ttl', '15', 'yes'),
(304, 'litespeed.conf.cdn', '', 'yes'),
(305, 'litespeed.conf.cdn-ori', 'a:0:{}', 'yes'),
(306, 'litespeed.conf.cdn-ori_dir', 'a:2:{i:0;s:10:\"wp-content\";i:1;s:11:\"wp-includes\";}', 'yes'),
(307, 'litespeed.conf.cdn-exc', 'a:0:{}', 'yes'),
(308, 'litespeed.conf.cdn-quic', '', 'yes'),
(309, 'litespeed.conf.cdn-cloudflare', '', 'yes'),
(310, 'litespeed.conf.cdn-cloudflare_email', '', 'yes'),
(311, 'litespeed.conf.cdn-cloudflare_key', '', 'yes'),
(312, 'litespeed.conf.cdn-cloudflare_name', '', 'yes'),
(313, 'litespeed.conf.cdn-cloudflare_zone', '', 'yes'),
(314, 'litespeed.conf.cdn-mapping', 'a:1:{i:0;a:5:{s:3:\"url\";b:0;s:7:\"inc_img\";s:1:\"1\";s:7:\"inc_css\";s:1:\"1\";s:6:\"inc_js\";s:1:\"1\";s:8:\"filetype\";a:17:{i:0;s:4:\".aac\";i:1;s:4:\".css\";i:2;s:4:\".eot\";i:3;s:4:\".gif\";i:4;s:5:\".jpeg\";i:5;s:3:\".js\";i:6;s:4:\".jpg\";i:7;s:5:\".less\";i:8;s:4:\".mp3\";i:9;s:4:\".mp4\";i:10;s:4:\".ogg\";i:11;s:4:\".otf\";i:12;s:4:\".pdf\";i:13;s:4:\".png\";i:14;s:4:\".svg\";i:15;s:4:\".ttf\";i:16;s:5:\".woff\";}}}', 'yes'),
(315, 'litespeed.conf.cdn-attr', 'a:5:{i:0;s:4:\".src\";i:1;s:9:\".data-src\";i:2;s:5:\".href\";i:3;s:7:\".poster\";i:4;s:13:\"source.srcset\";}', 'yes'),
(317, 'recently_activated', 'a:2:{s:35:\"litespeed-cache/litespeed-cache.php\";i:1618425511;i:0;b:0;}', 'yes'),
(321, 'ai1wm_secret_key', 'JpdWezFNEaFv', 'yes'),
(324, 'action_scheduler_hybrid_store_demarkation', '4', 'yes'),
(325, 'schema-ActionScheduler_StoreSchema', '3.0.1618425519', 'yes'),
(326, 'schema-ActionScheduler_LoggerSchema', '2.0.1618425519', 'yes'),
(329, 'wpforms_version', '1.6.6', 'yes'),
(330, 'wpforms_version_lite', '1.6.6', 'yes'),
(331, 'wpforms_activated', 'a:1:{s:4:\"lite\";i:1618425519;}', 'yes'),
(337, 'widget_wpforms-widget', 'a:1:{s:12:\"_multiwidget\";i:1;}', 'yes'),
(340, 'aioseo_options_internal', '{\"internal\":{\"validLicenseKey\":null,\"lastActiveVersion\":\"4.1.0.1\",\"migratedVersion\":\"0.0\",\"siteAnalysis\":{\"connectToken\":\"8bb99d19-e532-5a91-8ca1-67281ec09170\",\"score\":67,\"results\":\"{\\\"basic\\\":{\\\"title\\\":{\\\"status\\\":\\\"passed\\\",\\\"value\\\":\\\"banjaraclothing \\\\u2013 Just another WordPress site\\\",\\\"length\\\":45},\\\"description\\\":{\\\"status\\\":\\\"error\\\",\\\"value\\\":\\\"\\\",\\\"length\\\":0,\\\"error\\\":\\\"description-missing\\\"},\\\"keywords\\\":{\\\"search\\\":5,\\\"wordpress\\\":5,\\\"world\\\":4,\\\"post\\\":3,\\\"june\\\":2,\\\"categories\\\":2,\\\"comments\\\":2,\\\"feed\\\":2,\\\"recent\\\":2,\\\"uncategorized\\\":2},\\\"keywordsInTitleDescription\\\":{\\\"status\\\":\\\"error\\\",\\\"value\\\":{\\\"title\\\":{\\\"wordpress\\\":1},\\\"description\\\":[]},\\\"error\\\":\\\"description-missing\\\"},\\\"h1Tags\\\":{\\\"status\\\":\\\"passed\\\",\\\"value\\\":[\\\"banjaraclothing\\\"]},\\\"h2Tags\\\":{\\\"status\\\":\\\"passed\\\",\\\"value\\\":[\\\"Hello world!\\\",\\\"Recent Posts\\\",\\\"Recent Comments\\\",\\\"Archives\\\",\\\"Categories\\\",\\\"Meta\\\"]},\\\"noImgAltAtts\\\":{\\\"status\\\":\\\"passed\\\",\\\"value\\\":[]},\\\"linksRatio\\\":{\\\"status\\\":\\\"passed\\\",\\\"value\\\":{\\\"internal\\\":13,\\\"external\\\":1}}},\\\"advanced\\\":{\\\"searchPreview\\\":\\\"https:\\\\\\/\\\\\\/banjaraclothing.com\\\\\\/banjaraclothing \\\\u2013 Just another WordPress site\\\",\\\"mobileSearchPreview\\\":\\\"https:\\\\\\/\\\\\\/banjaraclothing.com\\\\\\/banjaraclothing \\\\u2013 Just another WordPress site\\\",\\\"mobileSnapshot\\\":\\\"https:\\\\\\/\\\\\\/image.thum.io\\\\\\/get\\\\\\/auth\\\\\\/11036-aioseo\\\\\\/iphone6\\\\\\/https:\\\\\\/\\\\\\/banjaraclothing.com\\\",\\\"canonicalTag\\\":{\\\"status\\\":\\\"warning\\\",\\\"value\\\":\\\"\\\",\\\"error\\\":\\\"canonical-missing\\\"},\\\"noindex\\\":{\\\"status\\\":\\\"passed\\\"},\\\"wwwCanonicalization\\\":{\\\"status\\\":\\\"passed\\\"},\\\"robotsRules\\\":{\\\"status\\\":\\\"passed\\\",\\\"value\\\":\\\"User-agent: *\\\\r\\\\nAllow: \\\\\\/wp-admin\\\\\\/admin-ajax.php\\\\r\\\\nDisallow: \\\\\\/wp-admin\\\\\\/\\\\r\\\\n\\\\r\\\\nSitemap: https:\\\\\\/\\\\\\/banjaraclothing.com\\\\\\/sitemap.xml\\\\r\\\\nSitemap: https:\\\\\\/\\\\\\/banjaraclothing.com\\\\\\/sitemap.rss\\\"},\\\"openGraph\\\":{\\\"status\\\":\\\"error\\\",\\\"error\\\":\\\"ogp-missing\\\",\\\"value\\\":[\\\"og:title\\\",\\\"og:type\\\",\\\"og:image\\\",\\\"og:url\\\"]},\\\"schema\\\":{\\\"status\\\":\\\"error\\\",\\\"error\\\":\\\"schema-missing\\\"}},\\\"performance\\\":{\\\"hasImgExpires\\\":{\\\"status\\\":\\\"passed\\\"},\\\"unminifiedJs\\\":{\\\"status\\\":\\\"error\\\",\\\"value\\\":[\\\"https:\\\\\\/\\\\\\/banjaraclothing.com\\\\\\/wp-content\\\\\\/themes\\\\\\/twentytwenty\\\\\\/assets\\\\\\/js\\\\\\/index.js\\\"],\\\"error\\\":\\\"js-unminified\\\"},\\\"unminifiedCss\\\":{\\\"status\\\":\\\"error\\\",\\\"value\\\":[\\\"https:\\\\\\/\\\\\\/banjaraclothing.com\\\\\\/wp-content\\\\\\/themes\\\\\\/twentytwenty\\\\\\/style.css\\\",\\\"https:\\\\\\/\\\\\\/banjaraclothing.com\\\\\\/wp-content\\\\\\/themes\\\\\\/twentytwenty\\\\\\/style.css\\\"],\\\"error\\\":\\\"css-unminified\\\"},\\\"pageObjects\\\":{\\\"status\\\":\\\"passed\\\",\\\"value\\\":{\\\"images\\\":0,\\\"js\\\":3,\\\"css\\\":3},\\\"total\\\":6},\\\"pageSize\\\":{\\\"status\\\":\\\"passed\\\",\\\"value\\\":9060},\\\"responseTime\\\":{\\\"status\\\":\\\"passed\\\",\\\"value\\\":0.040000000000000000832667268468867405317723751068115234375}},\\\"security\\\":{\\\"visiblePlugins\\\":{\\\"status\\\":\\\"passed\\\",\\\"value\\\":[]},\\\"visibleThemes\\\":{\\\"status\\\":\\\"warning\\\",\\\"value\\\":[\\\"Twentytwenty\\\"],\\\"error\\\":\\\"themes-visible\\\"},\\\"directoryListing\\\":{\\\"status\\\":\\\"error\\\",\\\"error\\\":\\\"directory-listing-open\\\"},\\\"googleSafeBrowsing\\\":{\\\"status\\\":\\\"passed\\\"},\\\"secureConnection\\\":{\\\"status\\\":\\\"passed\\\"}}}\",\"competitors\":[]},\"wizard\":\"{\\\"currentStage\\\":\\\"smart-recommendations\\\",\\\"stages\\\":[\\\"category\\\",\\\"additional-information\\\",\\\"features\\\",\\\"search-appearance\\\",\\\"smart-recommendations\\\",\\\"license-key\\\"],\\\"importers\\\":[],\\\"category\\\":{\\\"category\\\":\\\"online-store\\\",\\\"categoryOther\\\":null,\\\"siteTitle\\\":\\\"#site_title#current_day #separator_sa #tagline&nbsp;#tagline\\\",\\\"metaDescription\\\":\\\"#author_first_name #site_title\\\"},\\\"additionalInformation\\\":{\\\"siteRepresents\\\":\\\"organization\\\",\\\"person\\\":null,\\\"organizationName\\\":\\\"BanjaraClothing\\\",\\\"organizationLogo\\\":\\\"\\\",\\\"personName\\\":null,\\\"personLogo\\\":null,\\\"phone\\\":\\\"+918764019502\\\",\\\"contactType\\\":\\\"Sales\\\",\\\"contactTypeManual\\\":null,\\\"socialShareImage\\\":\\\"\\\",\\\"social\\\":{\\\"profiles\\\":{\\\"sameUsername\\\":{\\\"enable\\\":false,\\\"username\\\":null,\\\"included\\\":[\\\"facebookPageUrl\\\",\\\"twitterUrl\\\",\\\"pinterestUrl\\\",\\\"instagramUrl\\\",\\\"youtubeUrl\\\",\\\"linkedinUrl\\\"]},\\\"urls\\\":{\\\"facebookPageUrl\\\":null,\\\"twitterUrl\\\":null,\\\"instagramUrl\\\":null,\\\"pinterestUrl\\\":null,\\\"youtubeUrl\\\":null,\\\"linkedinUrl\\\":null,\\\"tumblrUrl\\\":null,\\\"yelpPageUrl\\\":null,\\\"soundCloudUrl\\\":null,\\\"wikipediaUrl\\\":null,\\\"myspaceUrl\\\":null,\\\"googlePlacesUrl\\\":null}}}},\\\"features\\\":[\\\"optimized-search-appearance\\\",\\\"analytics\\\",\\\"image-seo\\\",\\\"sitemaps\\\",\\\"video-sitemap\\\",\\\"local-seo\\\",\\\"news-sitemap\\\",\\\"redirects\\\"],\\\"searchAppearance\\\":{\\\"underConstruction\\\":false,\\\"postTypes\\\":{\\\"postTypes\\\":{\\\"all\\\":true,\\\"included\\\":[\\\"post\\\",\\\"page\\\",\\\"attachment\\\",\\\"product\\\"]}},\\\"multipleAuthors\\\":true,\\\"redirectAttachmentPages\\\":true},\\\"smartRecommendations\\\":{\\\"accountInfo\\\":\\\"hingerharsh@gmail.com\\\",\\\"usageTracking\\\":true},\\\"licenseKey\\\":null}\",\"category\":\"online-store\",\"categoryOther\":null,\"deprecatedOptions\":[]},\"integrations\":{\"semrush\":{\"accessToken\":null,\"tokenType\":null,\"expires\":null,\"refreshToken\":null}}}', 'yes'),
(341, 'aioseo_options_internal_lite', '{\"internal\":{\"activated\":1618425522,\"firstActivated\":1618425522,\"installed\":0,\"connect\":{\"key\":null,\"time\":0,\"network\":false,\"token\":null}}}', 'yes'),
(344, 'optin_monster_api', 'a:12:{s:3:\"api\";a:0:{}s:6:\"optins\";a:0:{}s:10:\"is_expired\";b:0;s:11:\"is_disabled\";b:0;s:10:\"is_invalid\";b:0;s:9:\"installed\";i:1618425524;s:9:\"connected\";s:0:\"\";s:4:\"beta\";b:0;s:12:\"auto_updates\";s:0:\"\";s:14:\"usage_tracking\";b:0;s:18:\"hide_announcements\";b:0;s:7:\"welcome\";a:1:{s:6:\"status\";s:4:\"none\";}}', 'yes'),
(345, 'omapi_review', 'a:2:{s:4:\"time\";i:1618425524;s:9:\"dismissed\";b:0;}', 'yes'),
(351, 'widget_optin-monster-api', 'a:1:{s:12:\"_multiwidget\";i:1;}', 'yes'),
(352, 'widget_monsterinsights-popular-posts-widget', 'a:1:{s:12:\"_multiwidget\";i:1;}', 'yes'),
(355, 'monsterinsights_over_time', 'a:3:{s:17:\"installed_version\";s:6:\"7.17.0\";s:14:\"installed_date\";i:1618425673;s:13:\"installed_pro\";b:0;}', 'yes'),
(356, 'monsterinsights_db_version', '7.4.0', 'yes'),
(357, 'monsterinsights_current_version', '7.17.0', 'yes'),
(358, 'monsterinsights_settings', 'a:42:{s:22:\"enable_affiliate_links\";b:1;s:15:\"affiliate_links\";a:2:{i:0;a:2:{s:4:\"path\";s:4:\"/go/\";s:5:\"label\";s:9:\"affiliate\";}i:1;a:2:{s:4:\"path\";s:11:\"/recommend/\";s:5:\"label\";s:9:\"affiliate\";}}s:12:\"demographics\";i:1;s:12:\"ignore_users\";a:2:{i:0;s:13:\"administrator\";i:1;s:6:\"editor\";}s:19:\"dashboards_disabled\";i:0;s:13:\"anonymize_ips\";i:0;s:19:\"extensions_of_files\";s:34:\"doc,pdf,ppt,zip,xls,docx,pptx,xlsx\";s:18:\"subdomain_tracking\";s:0:\"\";s:16:\"link_attribution\";b:1;s:16:\"tag_links_in_rss\";b:1;s:12:\"allow_anchor\";i:0;s:16:\"add_allow_linker\";i:0;s:11:\"custom_code\";s:0:\"\";s:13:\"save_settings\";a:1:{i:0;s:13:\"administrator\";}s:12:\"view_reports\";a:2:{i:0;s:13:\"administrator\";i:1;s:6:\"editor\";}s:11:\"events_mode\";s:2:\"js\";s:13:\"tracking_mode\";s:4:\"gtag\";s:30:\"gtagtracker_compatibility_mode\";b:1;s:15:\"email_summaries\";s:2:\"on\";s:23:\"summaries_html_template\";s:3:\"yes\";s:25:\"summaries_email_addresses\";a:1:{i:0;a:1:{s:5:\"email\";s:21:\"hingerharsh@gmail.com\";}}s:17:\"automatic_updates\";s:4:\"none\";s:26:\"popular_posts_inline_theme\";s:5:\"alpha\";s:26:\"popular_posts_widget_theme\";s:5:\"alpha\";s:28:\"popular_posts_products_theme\";s:5:\"alpha\";s:30:\"popular_posts_inline_placement\";s:6:\"manual\";s:34:\"popular_posts_widget_theme_columns\";s:1:\"2\";s:36:\"popular_posts_products_theme_columns\";s:1:\"2\";s:26:\"popular_posts_widget_count\";s:1:\"4\";s:28:\"popular_posts_products_count\";s:1:\"4\";s:38:\"popular_posts_widget_theme_meta_author\";s:2:\"on\";s:36:\"popular_posts_widget_theme_meta_date\";s:2:\"on\";s:40:\"popular_posts_widget_theme_meta_comments\";s:2:\"on\";s:39:\"popular_posts_products_theme_meta_price\";s:2:\"on\";s:40:\"popular_posts_products_theme_meta_rating\";s:2:\"on\";s:39:\"popular_posts_products_theme_meta_image\";s:2:\"on\";s:32:\"popular_posts_inline_after_count\";s:3:\"150\";s:36:\"popular_posts_inline_multiple_number\";s:1:\"3\";s:38:\"popular_posts_inline_multiple_distance\";s:3:\"250\";s:39:\"popular_posts_inline_multiple_min_words\";s:3:\"100\";s:31:\"popular_posts_inline_post_types\";a:1:{i:0;s:4:\"post\";}s:31:\"popular_posts_widget_post_types\";a:1:{i:0;s:4:\"post\";}}', 'yes'),
(360, 'monsterinsights_usage_tracking_config', 'a:6:{s:3:\"day\";i:6;s:4:\"hour\";i:22;s:6:\"minute\";i:11;s:6:\"second\";i:9;s:6:\"offset\";i:598269;s:8:\"initsend\";i:1619302269;}', 'yes'),
(361, 'action_scheduler_migration_status', 'complete', 'yes'),
(366, 'action_scheduler_lock_async-request-runner', '1619651432', 'yes'),
(368, 'monsterinsights_notifications', 'a:4:{s:6:\"update\";i:1618425674;s:4:\"feed\";a:0:{}s:6:\"events\";a:0:{}s:9:\"dismissed\";a:0:{}}', 'yes');
INSERT INTO `wp_options` (`option_id`, `option_name`, `option_value`, `autoload`) VALUES
(369, '_aioseo_cache_addons', 'a:5:{i:0;O:8:\"stdClass\":14:{s:3:\"sku\";s:16:\"aioseo-image-seo\";s:4:\"name\";s:9:\"Image SEO\";s:7:\"version\";s:5:\"1.0.3\";s:5:\"image\";N;s:4:\"icon\";s:13:\"svg-image-seo\";s:6:\"levels\";a:5:{i:0;s:8:\"business\";i:1;s:6:\"agency\";i:2;s:4:\"plus\";i:3;s:3:\"pro\";i:4;s:5:\"elite\";}s:13:\"currentLevels\";a:3:{i:0;s:4:\"plus\";i:1;s:3:\"pro\";i:2;s:5:\"elite\";}s:15:\"requiresUpgrade\";b:1;s:11:\"description\";s:147:\"<p>Globally control the Title attribute and Alt text for images in your content. These attributes are essential for both accessibility and SEO.</p>\";s:18:\"descriptionVersion\";i:0;s:11:\"downloadUrl\";s:0:\"\";s:10:\"productUrl\";s:71:\"https://aioseo.com/docs/using-the-image-seo-features-in-all-in-one-seo/\";s:12:\"learnMoreUrl\";s:71:\"https://aioseo.com/docs/using-the-image-seo-features-in-all-in-one-seo/\";s:9:\"manageUrl\";s:44:\"https://route#aioseo-search-appearance:media\";}i:1;O:8:\"stdClass\":14:{s:3:\"sku\";s:20:\"aioseo-video-sitemap\";s:4:\"name\";s:13:\"Video Sitemap\";s:7:\"version\";s:5:\"1.0.6\";s:5:\"image\";N;s:4:\"icon\";s:16:\"svg-sitemaps-pro\";s:6:\"levels\";a:5:{i:0;s:10:\"individual\";i:1;s:8:\"business\";i:2;s:6:\"agency\";i:3;s:3:\"pro\";i:4;s:5:\"elite\";}s:13:\"currentLevels\";a:3:{i:0;s:6:\"agency\";i:1;s:3:\"pro\";i:2;s:5:\"elite\";}s:15:\"requiresUpgrade\";b:1;s:11:\"description\";s:242:\"<p>The Video Sitemap works in much the same way as the XML Sitemap module, it generates an XML Sitemap specifically for video content on your site. Search engines use this information to display rich snippet information in search results.</p>\";s:18:\"descriptionVersion\";i:0;s:11:\"downloadUrl\";s:0:\"\";s:10:\"productUrl\";s:54:\"https://aioseo.com/docs/how-to-create-a-video-sitemap/\";s:12:\"learnMoreUrl\";s:54:\"https://aioseo.com/docs/how-to-create-a-video-sitemap/\";s:9:\"manageUrl\";s:43:\"https://route#aioseo-sitemaps:video-sitemap\";}i:2;O:8:\"stdClass\":14:{s:3:\"sku\";s:21:\"aioseo-local-business\";s:4:\"name\";s:18:\"Local Business SEO\";s:7:\"version\";s:7:\"1.1.0.1\";s:5:\"image\";N;s:4:\"icon\";s:18:\"svg-local-business\";s:6:\"levels\";a:5:{i:0;s:8:\"business\";i:1;s:6:\"agency\";i:2;s:4:\"plus\";i:3;s:3:\"pro\";i:4;s:5:\"elite\";}s:13:\"currentLevels\";a:3:{i:0;s:4:\"plus\";i:1;s:3:\"pro\";i:2;s:5:\"elite\";}s:15:\"requiresUpgrade\";b:1;s:11:\"description\";s:252:\"<p>Local Business schema markup enables you to tell Google about your business, including your business name, address and phone number, opening hours and price range. This information may be displayed as a Knowledge Graph card or business carousel.</p>\";s:18:\"descriptionVersion\";i:0;s:11:\"downloadUrl\";s:0:\"\";s:10:\"productUrl\";s:43:\"https://aioseo.com/docs/local-business-seo/\";s:12:\"learnMoreUrl\";s:43:\"https://aioseo.com/docs/local-business-seo/\";s:9:\"manageUrl\";s:40:\"https://route#aioseo-local-seo:locations\";}i:3;O:8:\"stdClass\":14:{s:3:\"sku\";s:19:\"aioseo-news-sitemap\";s:4:\"name\";s:12:\"News Sitemap\";s:7:\"version\";s:5:\"1.0.3\";s:5:\"image\";N;s:4:\"icon\";s:16:\"svg-sitemaps-pro\";s:6:\"levels\";a:4:{i:0;s:8:\"business\";i:1;s:6:\"agency\";i:2;s:3:\"pro\";i:3;s:5:\"elite\";}s:13:\"currentLevels\";a:2:{i:0;s:3:\"pro\";i:1;s:5:\"elite\";}s:15:\"requiresUpgrade\";b:1;s:11:\"description\";s:283:\"<p>Our Google News Sitemap lets you control which content you submit to Google News and only contains articles that were published in the last 48 hours. In order to submit a News Sitemap to Google, you must have added your site to Google’s Publisher Center and had it approved.</p>\";s:18:\"descriptionVersion\";i:0;s:11:\"downloadUrl\";s:0:\"\";s:10:\"productUrl\";s:60:\"https://aioseo.com/docs/how-to-create-a-google-news-sitemap/\";s:12:\"learnMoreUrl\";s:60:\"https://aioseo.com/docs/how-to-create-a-google-news-sitemap/\";s:9:\"manageUrl\";s:42:\"https://route#aioseo-sitemaps:news-sitemap\";}i:4;O:8:\"stdClass\":14:{s:3:\"sku\";s:16:\"aioseo-redirects\";s:4:\"name\";s:19:\"Redirection Manager\";s:7:\"version\";s:5:\"1.0.0\";s:5:\"image\";N;s:4:\"icon\";s:480:\"PHN2ZyB2aWV3Qm94PSIwIDAgMjQgMjQiIGZpbGw9Im5vbmUiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgY2xhc3M9ImFpb3Nlby1yZWRpcmVjdCI+PHBhdGggZmlsbC1ydWxlPSJldmVub2RkIiBjbGlwLXJ1bGU9ImV2ZW5vZGQiIGQ9Ik0xMC41OSA5LjE3TDUuNDEgNEw0IDUuNDFMOS4xNyAxMC41OEwxMC41OSA5LjE3Wk0xNC41IDRMMTYuNTQgNi4wNEw0IDE4LjU5TDUuNDEgMjBMMTcuOTYgNy40NkwyMCA5LjVWNEgxNC41Wk0xMy40MiAxNC44MkwxNC44MyAxMy40MUwxNy45NiAxNi41NEwyMCAxNC41VjIwSDE0LjVMMTYuNTUgMTcuOTVMMTMuNDIgMTQuODJaIiBmaWxsPSJjdXJyZW50Q29sb3IiIC8+PC9zdmc+\";s:6:\"levels\";a:4:{i:0;s:6:\"agency\";i:1;s:8:\"business\";i:2;s:3:\"pro\";i:3;s:5:\"elite\";}s:13:\"currentLevels\";a:2:{i:0;s:3:\"pro\";i:1;s:5:\"elite\";}s:15:\"requiresUpgrade\";b:1;s:11:\"description\";s:100:\"<p>Our Redirection Manager allows you to create and manage redirects for 404s or modified posts.</p>\";s:18:\"descriptionVersion\";i:0;s:11:\"downloadUrl\";s:0:\"\";s:10:\"productUrl\";s:34:\"https://aioseo.com/docs/redirects/\";s:12:\"learnMoreUrl\";s:34:\"https://aioseo.com/docs/redirects/\";s:9:\"manageUrl\";s:30:\"https://route#aioseo-redirects\";}}', 'no'),
(370, '_aioseo_cache_expiration_addons', '1618512612', 'no'),
(371, '_aioseo_cache_admin_help_docs', '{\"categories\":{\"getting-started\":\"Getting Started\",\"advanced-settings\":\"Advanced Settings\",\"display-settings\":\"Display Settings\",\"general-seo-topics\":\"General SEO Topics\",\"feature-manager\":\"Feature Manager\",\"installation\":\"Installation\"},\"docs\":{\"35609\":{\"title\":\"Choosing Which Redirect Type to Use\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/choosing-which-redirect-type-to-use\\/\",\"categories\":[\"redirection-manager\"]},\"35604\":{\"title\":\"Automatic Redirects When URLs Change in Content\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/automatic-redirects-when-urls-change-in-content\\/\",\"categories\":[\"redirection-manager\"]},\"35599\":{\"title\":\"Importing Redirects From Other Plugins\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/importing-redirects-from-other-plugins\\/\",\"categories\":[\"redirection-manager\"]},\"35588\":{\"title\":\"Exporting and Importing Redirects\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/exporting-and-importing-redirects\\/\",\"categories\":[\"redirection-manager\"]},\"35579\":{\"title\":\"Logging 404 Errors in All in One SEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/logging-404-errors-in-all-in-one-seo\\/\",\"categories\":[\"redirection-manager\"]},\"35570\":{\"title\":\"Redirect GDPR Privacy Information\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/redirect-gdpr-privacy-information\\/\",\"categories\":[\"redirection-manager\"]},\"35552\":{\"title\":\"Logging Redirects in All in One SEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/logging-redirects-in-all-in-one-seo\\/\",\"categories\":[\"redirection-manager\"]},\"35133\":{\"title\":\"aioseo_twitter_tags\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_twitter_tags\\/\",\"categories\":[\"filter-hooks\"]},\"35132\":{\"title\":\"aioseo_facebook_tags\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_facebook_tags\\/\",\"categories\":[\"filter-hooks\"]},\"34993\":{\"title\":\"Ignoring Case Sensitivity in Redirects\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/ignoring-case-sensitivity-in-redirects\\/\",\"categories\":[\"redirection-manager\"]},\"34983\":{\"title\":\"Ignoring the Trailing Slash in Redirects\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/ignoring-the-trailing-slash-in-redirects\\/\",\"categories\":[\"redirection-manager\"]},\"34977\":{\"title\":\"How to Redirect Multiple URLs to the Same Destination\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-redirect-multiple-urls-to-the-same-destination\\/\",\"categories\":[\"redirection-manager\"]},\"34923\":{\"title\":\"How to Redirect a Post or Page in All in One SEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-redirect-a-post-or-page-in-all-in-one-seo\\/\",\"categories\":[\"redirection-manager\"]},\"34701\":{\"title\":\"Adding WooCommerce Product Attributes to SEO Title or Description\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/adding-woocommerce-product-attributes-to-seo-title-or-description\\/\",\"categories\":[\"post-page-settings\",\"search-appearance\",\"woocommerce\"]},\"34179\":{\"title\":\"Using the Smart Tags in Titles and Descriptions\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/using-the-smart-tags-in-titles-and-descriptions\\/\",\"categories\":[\"post-page-settings\",\"search-appearance\"]},\"33507\":{\"title\":\"What\'s The Difference Between TruSEO and Page Analysis?\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/whats-the-difference-between-truseo-and-page-analysis\\/\",\"categories\":[\"frequently-asked-questions\",\"post-page-settings\",\"truseo\"]},\"33310\":{\"title\":\"Setting Noindex for RSS Feeds\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-noindex-for-rss-feeds\\/\",\"categories\":[\"advanced-settings\",\"search-appearance\"]},\"33130\":{\"title\":\"aioseo_disable_shortcode_parsing\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_disable_shortcode_parsing\\/\",\"categories\":[\"filter-hooks\"]},\"32085\":{\"title\":\"aioseo_conflicting_shortcodes\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_conflicting_shortcodes\\/\",\"categories\":[\"filter-hooks\"]},\"31992\":{\"title\":\"aioseo_schema_graphs\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_schema_graphs\\/\",\"categories\":[\"filter-hooks\"]},\"31589\":{\"title\":\"Understanding the TruSEO Page Analysis Recommendations\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/understanding-the-truseo-page-analysis-recommendations\\/\",\"categories\":[\"post-page-settings\",\"truseo\"]},\"31042\":{\"title\":\"Getting Keyphrase Suggestions From Semrush\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/getting-keyphrase-suggestions-from-semrush\\/\",\"categories\":[\"post-page-settings\",\"truseo\"]},\"30728\":{\"title\":\"Unable to Save Settings Due to Cloudflare Firewall Rules\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/unable-to-save-settings-due-to-cloudflare-firewall-rules\\/\",\"categories\":[\"troubleshooting\"]},\"30318\":{\"title\":\"aioseo_flush_output_buffer\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_flush_output_buffer\\/\",\"categories\":[\"filter-hooks\"]},\"18813\":{\"title\":\"Installing All in One SEO Pro\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/installing-all-in-one-seo-pro\\/\",\"categories\":[\"getting-started\",\"installation\"]},\"18973\":{\"title\":\"Beginners Guide for All in One SEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/quick-start-guide\\/\",\"categories\":[\"getting-started\"]},\"18820\":{\"title\":\"Setting the SEO Title and Description for Your Content\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-seo-title-and-description-for-your-content\\/\",\"categories\":[\"getting-started\",\"post-page-settings\"]},\"18902\":{\"title\":\"How to Create an XML Sitemap\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-create-an-xml-sitemap\\/\",\"categories\":[\"getting-started\",\"xml-sitemap\"]},\"18859\":{\"title\":\"Beginners Guide to Social Networks Settings for Facebook\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/beginners-guide-to-social-networks-settings-for-facebook\\/\",\"categories\":[\"facebook-settings\",\"getting-started\",\"social-networks\"]},\"18857\":{\"title\":\"Beginners Guide to Social Networks Settings for Twitter\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/beginners-guide-to-social-networks-settings-for-twitter\\/\",\"categories\":[\"getting-started\",\"social-networks\",\"twitter-settings\"]},\"29991\":{\"title\":\"aioseo_disable_link_format\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_disable_link_format\\/\",\"categories\":[\"filter-hooks\"]},\"27841\":{\"title\":\"aioseo_thumbnail_size\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_thumbnail_size\\/\",\"categories\":[\"filter-hooks\"]},\"27844\":{\"title\":\"Displaying Additional Twitter Data for Written By and Reading Time\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/displaying-additional-twitter-data-for-written-by-and-reading-time\\/\",\"categories\":[\"social-networks\",\"twitter-settings\"]},\"27494\":{\"title\":\"aioseo_meta_views\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_meta_views\\/\",\"categories\":[\"filter-hooks\"]},\"27363\":{\"title\":\"Using the SEO Analysis Tool\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/using-the-seo-analysis-tool\\/\",\"categories\":[\"seo-analysis\"]},\"27272\":{\"title\":\"Importing Settings From Other Plugins\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/importing-settings-from-other-plugins\\/\",\"categories\":[\"importer-exporter\",\"seo-data-importer\"]},\"27268\":{\"title\":\"Backing Up and Restoring AIOSEO Settings\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/backing-up-and-restoring-aioseo-settings\\/\",\"categories\":[\"importer-exporter\"]},\"27259\":{\"title\":\"Importing and Exporting AIOSEO Settings and Meta Data\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/importing-and-exporting-aioseo-settings-and-meta-data\\/\",\"categories\":[\"importer-exporter\"]},\"26641\":{\"title\":\"Setting FAQ Page Schema Markup in Your Content\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-faq-page-schema-markup-in-your-content\\/\",\"categories\":[\"schema-settings\"]},\"26640\":{\"title\":\"Setting Software Application Schema Markup in Your Content\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-software-application-schema-markup-in-your-content\\/\",\"categories\":[\"schema-settings\"]},\"26639\":{\"title\":\"Setting Recipe Schema Markup in Your Content\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-recipe-schema-markup-in-your-content\\/\",\"categories\":[\"schema-settings\"]},\"26638\":{\"title\":\"Setting Product Schema Markup in Your Content\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-product-schema-markup-in-your-content\\/\",\"categories\":[\"schema-settings\"]},\"26637\":{\"title\":\"Setting Course Schema Markup in Your Content\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-course-schema-markup-in-your-content\\/\",\"categories\":[\"schema-settings\"]},\"26450\":{\"title\":\"Blank Title Formats Have Been Detected\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/blank-title-formats-detected\\/\",\"categories\":[\"troubleshooting\"]},\"25802\":{\"title\":\"aioseo_sitemap_additional_pages\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_sitemap_additional_pages\\/\",\"categories\":[\"filter-hooks\"]},\"24928\":{\"title\":\"Including Custom Fields in the TruSEO Page Analysis\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/including-custom-fields-in-the-seo-page-analysis\\/\",\"categories\":[\"content-type-settings\",\"search-appearance\",\"truseo\"]},\"24285\":{\"title\":\"aioseo_prev_link\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_prev_link\\/\",\"categories\":[\"filter-hooks\"]},\"24284\":{\"title\":\"aioseo_next_link\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_next_link\\/\",\"categories\":[\"filter-hooks\"]},\"23717\":{\"title\":\"aioseo_canonical_url\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_canonical_url\\/\",\"categories\":[\"filter-hooks\"]},\"23604\":{\"title\":\"aioseo_schema_breadcrumbs_home\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_schema_breadcrumbs_home\\/\",\"categories\":[\"filter-hooks\"]},\"23448\":{\"title\":\"aioseo_schema_disable\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_schema_disable\\/\",\"categories\":[\"filter-hooks\"]},\"23447\":{\"title\":\"aioseo_robots_meta\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_robots_meta\\/\",\"categories\":[\"filter-hooks\"]},\"23446\":{\"title\":\"aioseo_disable\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_disable\\/\",\"categories\":[\"filter-hooks\"]},\"23441\":{\"title\":\"aioseo_generate_descriptions_from_content\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_generate_descriptions_from_content\\/\",\"categories\":[\"filter-hooks\"]},\"23438\":{\"title\":\"aioseo_disable_title_rewrites\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_disable_title_rewrites\\/\",\"categories\":[\"filter-hooks\"]},\"23437\":{\"title\":\"aioseo_post_metabox_priority\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_post_metabox_priority\\/\",\"categories\":[\"filter-hooks\"]},\"23436\":{\"title\":\"aioseo_show_seo_news\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_show_seo_news\\/\",\"categories\":[\"filter-hooks\"]},\"23433\":{\"title\":\"aioseo_show_in_admin_bar\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_show_in_admin_bar\\/\",\"categories\":[\"filter-hooks\"]},\"23423\":{\"title\":\"aioseo_keywords\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_keywords\\/\",\"categories\":[\"filter-hooks\"]},\"23350\":{\"title\":\"aioseo_title\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_title\\/\",\"categories\":[\"filter-hooks\"]},\"23351\":{\"title\":\"aioseo_description\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo_description\\/\",\"categories\":[\"filter-hooks\"]},\"23415\":{\"title\":\"Troubleshooting Action Scheduler issues with AIOSEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/troubleshooting-action-scheduler-issues\\/\",\"categories\":[\"troubleshooting\"]},\"20504\":{\"title\":\"Where Did my SEO Keywords go in All in One SEO v4.0?\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/where-did-my-seo-keywords-go-in-all-in-one-seo-v4-0\\/\",\"categories\":[\"advanced-settings\",\"frequently-asked-questions\",\"post-page-settings\",\"search-appearance\"]},\"18792\":{\"title\":\"Sitemap rewrite rules for NGINX\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/xml-sitemap-rewrite-rules-for-nginx\\/\",\"categories\":[\"rss-sitemap\",\"video-sitemap\",\"xml-sitemap\"]},\"18793\":{\"title\":\"Unfiltered HTML Capability is Required\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/unfiltered-html-capability\\/\",\"categories\":[\"troubleshooting\"]},\"18794\":{\"title\":\"Deprecated Open Graph Settings in All in One SEO version 4.0\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/deprecated-opengraph-settings\\/\",\"categories\":[\"social-networks\"]},\"18795\":{\"title\":\"Why does the character counter for SEO titles show a different count?\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/why-does-the-character-counter-for-seo-titles-show-a-different-count\\/\",\"categories\":[\"frequently-asked-questions\",\"post-page-settings\"]},\"18796\":{\"title\":\"Adding nofollow, sponsored, UGC and title attributes to links\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/adding-nofollow-sponsored-and-title-attributes-to-links\\/\",\"categories\":[\"post-page-settings\"]},\"18797\":{\"title\":\"Setting the SEO for WooCommerce Content\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-seo-for-woocommerce-content\\/\",\"categories\":[\"search-appearance\",\"woocommerce\"]},\"18798\":{\"title\":\"All in One SEO uses the WordPress REST API\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/aioseo-uses-rest-api\\/\",\"categories\":[\"frequently-asked-questions\"]},\"18799\":{\"title\":\"How to Remove All Settings and Data When you Uninstall All in One SEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-remove-all-settings-and-data-when-you-uninstall-all-in-one-seo\\/\",\"categories\":[\"advanced-settings\",\"general-settings\"]},\"18800\":{\"title\":\"How to Disable TruSEO Content Analysis\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-disable-truseo-content-analysis\\/\",\"categories\":[\"advanced-settings\",\"general-settings\",\"truseo\"]},\"18801\":{\"title\":\"Enabling Automatic Updates for All in One SEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/enabling-automatic-updates-for-all-in-one-seo\\/\",\"categories\":[\"advanced-settings\",\"general-settings\"]},\"18802\":{\"title\":\"Hiding Plugin Notifications in the Notifications Center\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/hiding-plugin-notifications-in-the-notifications-center\\/\",\"categories\":[\"advanced-settings\",\"general-settings\"]},\"18803\":{\"title\":\"How to Hide the AIOSEO Settings on the Edit Content Screens in WordPress\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-hide-the-aioseo-settings-on-the-edit-content-screens-in-wordpress\\/\",\"categories\":[\"advanced-settings\",\"content-type-settings\",\"post-page-settings\",\"search-appearance\"]},\"18804\":{\"title\":\"Setting Noindex and Nofollow on Paginated Content\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-noindex-and-nofollow-on-paginated-content\\/\",\"categories\":[\"advanced-settings\",\"search-appearance\"]},\"18805\":{\"title\":\"Setting Unique SEO Titles and Descriptions for Paginated Content\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-unique-seo-titles-and-descriptions-for-paginated-content\\/\",\"categories\":[\"advanced-settings\",\"search-appearance\"]},\"18806\":{\"title\":\"Setting the SEO Title and Description Format for Custom Post Type Archives\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-seo-title-and-description-format-for-custom-post-type-archives\\/\",\"categories\":[\"archive-settings\",\"search-appearance\"]},\"18807\":{\"title\":\"Keyword Settings\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/keyword-settings\\/\",\"categories\":[\"advanced-settings\",\"search-appearance\"]},\"18808\":{\"title\":\"Using the Quick Edit Feature in All in One SEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/using-the-quick-edit-feature-in-all-in-one-seo\\/\",\"categories\":[\"post-page-settings\"]},\"18809\":{\"title\":\"How to FTP to your web server\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-ftp-to-your-web-server\\/\",\"categories\":[\"frequently-asked-questions\"]},\"18810\":{\"title\":\"How to manually install All in One SEO Pro when the file is too big\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-manually-install-all-in-one-seo-pro-when-the-file-is-too-big\\/\",\"categories\":[\"frequently-asked-questions\",\"installation\"]},\"18811\":{\"title\":\"How to Upgrade From All in One SEO Lite to Pro\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-upgrade-from-all-in-one-seo-lite-to-pro\\/\",\"categories\":[\"getting-started\",\"installation\"]},\"18812\":{\"title\":\"Installation instructions for WordPress.com Users\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/installation-instructions-for-wordpress-com-users\\/\",\"categories\":[\"installation\"]},\"18814\":{\"title\":\"Configuring the Twitter Settings for Your Content\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/configuring-the-twitter-settings-for-your-content\\/\",\"categories\":[\"post-page-settings\",\"social-networks\",\"twitter-settings\"]},\"18815\":{\"title\":\"Configuring the Facebook Settings for Your Content\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/configuring-the-facebook-settings-for-your-content\\/\",\"categories\":[\"facebook-settings\",\"post-page-settings\",\"social-networks\"]},\"18816\":{\"title\":\"Hiding the AIOSEO Column on Taxonomy Screens\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/hiding-the-aioseo-column-on-taxonomy-screens\\/\",\"categories\":[\"advanced-settings\",\"category-tag-settings\",\"general-settings\"]},\"18817\":{\"title\":\"Setting the Schema Type for Individual Content\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-schema-type-for-individual-content\\/\",\"categories\":[\"post-page-settings\",\"schema-settings\"]},\"18818\":{\"title\":\"Setting the Sitemap Priority and Frequency for Individual Content\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-sitemap-priority-and-frequency-for-individual-content\\/\",\"categories\":[\"post-page-settings\",\"xml-sitemap\"]},\"18819\":{\"title\":\"Setting the Robots Meta for Individual Content\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-robots-meta-for-individual-content\\/\",\"categories\":[\"post-page-settings\"]},\"18821\":{\"title\":\"Individual Post\\/Page Settings\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/post-settings\\/\",\"categories\":[\"post-page-settings\"]},\"18822\":{\"title\":\"Bad Bot Blocker\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/bad-bot-blocker\\/\",\"categories\":[\"bad-bot-blocker\"]},\"18823\":{\"title\":\"How to Fix a 404 Error When Viewing Your Sitemap\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-fix-a-404-error-when-viewing-your-sitemap\\/\",\"categories\":[\"frequently-asked-questions\",\"google-news-sitemap\",\"rss-sitemap\",\"troubleshooting\",\"video-sitemap\",\"xml-sitemap\"]},\"18824\":{\"title\":\"Local Business SEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/local-business-seo\\/\",\"categories\":[\"local-business-seo\",\"schema-settings\"]},\"18825\":{\"title\":\"When to use NOINDEX or the robots.txt?\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/when-to-use-noindex-or-the-robots-txt\\/\",\"categories\":[\"frequently-asked-questions\",\"robots-txt\",\"search-appearance\"]},\"18826\":{\"title\":\"Support for Videos Embedded Using the Media Library\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/support-for-videos-embedded-using-the-media-library\\/\",\"categories\":[\"video-sitemap\"]},\"18827\":{\"title\":\"Supported Videos\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/supported-videos\\/\",\"categories\":[\"video-sitemap\"]},\"18828\":{\"title\":\"Performance Settings\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/performance-settings\\/\",\"categories\":[\"performance\"]},\"18829\":{\"title\":\"Using the Image SEO Features in All in One SEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/using-the-image-seo-features-in-all-in-one-seo\\/\",\"categories\":[\"image-seo\",\"media-settings\",\"search-appearance\"]},\"18830\":{\"title\":\"Setting the SEO Title and Description Format for Author and Date Archives\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-seo-title-and-description-format-for-author-and-date-archives\\/\",\"categories\":[\"archive-settings\",\"search-appearance\"]},\"18831\":{\"title\":\"Custom Fields in Titles and Descriptions\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/custom-fields-in-titles-and-descriptions\\/\",\"categories\":[\"content-type-settings\",\"post-page-settings\",\"search-appearance\"]},\"18832\":{\"title\":\"Using the Focus Keyphrase to Analyze Your Content\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/using-the-focus-keyphrase-to-analyze-your-content\\/\",\"categories\":[\"frequently-asked-questions\",\"post-page-settings\",\"truseo\"]},\"18833\":{\"title\":\"Using the Robots.txt Tool in All in One SEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/using-the-robots-txt-tool-in-all-in-one-seo\\/\",\"categories\":[\"robots-txt\"]},\"18834\":{\"title\":\"Using the Robots Meta Settings in All in One SEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/using-the-robots-meta-settings-in-all-in-one-seo\\/\",\"categories\":[\"advanced-settings\",\"archive-settings\",\"category-tag-settings\",\"content-type-settings\",\"media-settings\",\"post-page-settings\",\"search-appearance\",\"taxonomy-settings\"]},\"18835\":{\"title\":\"Noindex Settings in All in One SEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/noindex-settings\\/\",\"categories\":[\"search-appearance\"]},\"18836\":{\"title\":\"Configuring the Schema Settings in All in One SEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/configuring-the-schema-settings-in-all-in-one-seo\\/\",\"categories\":[\"schema-settings\",\"search-appearance\"]},\"18837\":{\"title\":\"A Guide to Schema.org Markup for Rich Snippets\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/a-guide-to-schema-org-markup-for-rich-snippets\\/\",\"categories\":[\"general-seo-topics\",\"schema-settings\"]},\"18838\":{\"title\":\"Hiding the AIOSEO Admin Bar Menu\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/hiding-the-aioseo-admin-bar-menu\\/\",\"categories\":[\"advanced-settings\",\"general-settings\"]},\"18839\":{\"title\":\"Hiding the AIOSEO Dashboard Widget\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/hiding-the-aioseo-dashboard-widget\\/\",\"categories\":[\"advanced-settings\",\"general-settings\"]},\"18840\":{\"title\":\"Hiding the AIOSEO Column on All Posts Screens\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/hiding-the-aioseo-column-on-all-posts-screens\\/\",\"categories\":[\"advanced-settings\",\"general-settings\"]},\"18841\":{\"title\":\"Display Settings\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/display-settings\\/\",\"categories\":[\"display-settings\"]},\"18842\":{\"title\":\"Setting the SEO Title and Description Format for Media Attachments\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-seo-title-and-description-format-for-media-attachments\\/\",\"categories\":[\"media-settings\",\"search-appearance\"]},\"18843\":{\"title\":\"Showing or Hiding Your Content in Search Results\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/showing-or-hiding-your-content-in-search-results\\/\",\"categories\":[\"archive-settings\",\"category-tag-settings\",\"content-type-settings\",\"media-settings\",\"post-page-settings\",\"search-appearance\",\"taxonomy-settings\"]},\"18844\":{\"title\":\"Content Type Settings\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/custom-post-type-settings\\/\",\"categories\":[\"content-type-settings\",\"search-appearance\"]},\"18845\":{\"title\":\"What Are Media Attachments and Should I Submit Them to Search Engines?\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/what-are-media-attachments-and-should-i-submit-them-to-search-engines\\/\",\"categories\":[\"frequently-asked-questions\",\"media-settings\"]},\"18846\":{\"title\":\"Setting the SEO Title and Description Format for Custom Taxonomies\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-seo-title-and-description-format-for-custom-taxonomies\\/\",\"categories\":[\"category-tag-settings\",\"search-appearance\",\"taxonomy-settings\"]},\"18847\":{\"title\":\"Setting the SEO Title and Description Format for Custom Post Types\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-seo-title-and-description-format-for-custom-post-types\\/\",\"categories\":[\"content-type-settings\",\"search-appearance\"]},\"18848\":{\"title\":\"Setting the SEO Title and Description Format for Tags\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-seo-title-and-description-format-for-tags\\/\",\"categories\":[\"category-tag-settings\",\"search-appearance\",\"taxonomy-settings\"]},\"18849\":{\"title\":\"Setting the SEO Title and Description Format for Categories\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-seo-title-and-description-format-for-categories\\/\",\"categories\":[\"category-tag-settings\",\"search-appearance\",\"taxonomy-settings\"]},\"18850\":{\"title\":\"Setting the SEO Title and Description Format for Pages\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-seo-title-and-description-format-for-pages\\/\",\"categories\":[\"content-type-settings\",\"post-page-settings\",\"search-appearance\"]},\"18851\":{\"title\":\"Setting the SEO Title and Description Format for Posts\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-seo-title-and-description-format-for-posts\\/\",\"categories\":[\"content-type-settings\",\"post-page-settings\",\"search-appearance\"]},\"18852\":{\"title\":\"Title Settings\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/title-settings\\/\",\"categories\":[\"search-appearance\"]},\"18853\":{\"title\":\"Setting the SEO for Your Home Page\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-seo-for-your-home-page\\/\",\"categories\":[\"home-page-settings\",\"search-appearance\"]},\"18854\":{\"title\":\"General Settings\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/general-settings\\/\",\"categories\":[\"general-settings\"]},\"18855\":{\"title\":\"How to Add Your License Key in All in One SEO Pro\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-add-your-license-key-in-all-in-one-seo-pro\\/\",\"categories\":[\"general-settings\",\"getting-started\"]},\"18856\":{\"title\":\"Canonical URLs in All in One SEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/canonical-urls-in-all-in-one-seo\\/\",\"categories\":[\"advanced-settings\",\"search-appearance\"]},\"18858\":{\"title\":\"Adding non-WordPress Content to the Video Sitemap\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/adding-non-wordpress-content-to-the-video-sitemap\\/\",\"categories\":[\"video-sitemap\"]},\"18860\":{\"title\":\"Troubleshooting Problems With Sharing Content on Twitter\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/troubleshooting-problems-with-sharing-content-on-twitter\\/\",\"categories\":[\"social-networks\",\"troubleshooting\",\"twitter-settings\"]},\"18861\":{\"title\":\"Troubleshooting Problems With Sharing Content on Facebook\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/troubleshooting-problems-with-sharing-content-on-facebook\\/\",\"categories\":[\"facebook-settings\",\"social-networks\",\"troubleshooting\"]},\"18862\":{\"title\":\"Getting Started With Pinterest Rich Pins\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/using-social-meta-for-pinterest-rich-pins\\/\",\"categories\":[\"pinterest-settings\",\"social-networks\"]},\"18863\":{\"title\":\"Setting the Content Publisher for Twitter\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-content-publisher-for-twitter\\/\",\"categories\":[\"social-networks\",\"twitter-settings\"]},\"18864\":{\"title\":\"Exclude Images from XML Sitemap for Yandex\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/exclude-images-from-xml-sitemap-for-yandex\\/\",\"categories\":[\"xml-sitemap\"]},\"18865\":{\"title\":\"Submitting a Sitemap to Yandex\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/submitting-a-sitemap-to-yandex\\/\",\"categories\":[\"rss-sitemap\",\"video-sitemap\",\"xml-sitemap\"]},\"18866\":{\"title\":\"Submitting a Sitemap to Bing\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/submitting-a-sitemap-to-bing\\/\",\"categories\":[\"bing-webmaster-tools\",\"rss-sitemap\",\"video-sitemap\",\"xml-sitemap\"]},\"18867\":{\"title\":\"Submitting a Sitemap to Google\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/submitting-a-sitemap-to-google\\/\",\"categories\":[\"google-news-sitemap\",\"google-search-console\",\"rss-sitemap\",\"video-sitemap\",\"xml-sitemap\"]},\"18868\":{\"title\":\"Including Date and Author Archives in Your XML Sitemap\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/including-date-and-author-archives-in-your-xml-sitemap\\/\",\"categories\":[\"xml-sitemap\"]},\"18869\":{\"title\":\"Choosing Which Content to Include in Your Video Sitemap\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/choosing-which-content-to-include-in-your-video-sitemap\\/\",\"categories\":[\"video-sitemap\"]},\"18870\":{\"title\":\"Choosing Which Content to Include in Your XML Sitemap\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/choosing-which-content-to-include-in-your-xml-sitemap\\/\",\"categories\":[\"xml-sitemap\"]},\"18871\":{\"title\":\"Using Sitemap Indexes and Pagination\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/using-sitemap-indexes-and-pagination\\/\",\"categories\":[\"video-sitemap\",\"xml-sitemap\"]},\"18872\":{\"title\":\"How to Disable Sitemaps in All in One SEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-disable-sitemaps-in-all-in-one-seo\\/\",\"categories\":[\"google-news-sitemap\",\"rss-sitemap\",\"video-sitemap\",\"xml-sitemap\"]},\"18873\":{\"title\":\"Baidu Webmaster Tools Verification\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/baidu-webmaster-tools-verification\\/\",\"categories\":[\"webmaster-tools\",\"webmaster-verification\"]},\"18874\":{\"title\":\"Setting Twitter Social Meta for Your Homepage\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-twitter-social-meta-for-your-homepage\\/\",\"categories\":[\"home-page-settings\",\"social-networks\",\"twitter-settings\"]},\"18875\":{\"title\":\"Setting Facebook Social Meta for Your Homepage\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-facebook-social-meta-for-your-homepage\\/\",\"categories\":[\"facebook-settings\",\"home-page-settings\",\"social-networks\"]},\"18876\":{\"title\":\"Setting the Card Type for Twitter\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-card-type-for-twitter\\/\",\"categories\":[\"social-networks\",\"twitter-settings\"]},\"18877\":{\"title\":\"Setting the Object Types for Facebook\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-object-types-for-facebook\\/\",\"categories\":[\"facebook-settings\",\"social-networks\"]},\"18879\":{\"title\":\"Setting the Priority and Frequency for Content in the Video Sitemap\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-priority-and-frequency-for-content-in-the-video-sitemap\\/\",\"categories\":[\"video-sitemap\"]},\"18880\":{\"title\":\"Setting the Priority and Frequency for Content in the XML Sitemap\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-priority-and-frequency-for-content-in-the-xml-sitemap\\/\",\"categories\":[\"xml-sitemap\"]},\"18881\":{\"title\":\"How to Exclude Content from Your RSS Sitemap\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-exclude-content-from-your-rss-sitemap\\/\",\"categories\":[\"rss-sitemap\"]},\"18882\":{\"title\":\"How to Exclude Content from Your Google News Sitemap\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-exclude-content-from-your-google-news-sitemap\\/\",\"categories\":[\"google-news-sitemap\"]},\"18883\":{\"title\":\"How to Exclude Content from Your Video Sitemap\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-exclude-content-from-your-video-sitemap\\/\",\"categories\":[\"video-sitemap\"]},\"18884\":{\"title\":\"How to Exclude Content from Your XML Sitemap\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-exclude-content-from-your-xml-sitemap\\/\",\"categories\":[\"xml-sitemap\"]},\"18885\":{\"title\":\"Setting Article Tags for Facebook\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-article-tags-for-facebook\\/\",\"categories\":[\"facebook-settings\",\"social-networks\"]},\"18886\":{\"title\":\"Setting the Content Author for Twitter\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-content-author-for-twitter\\/\",\"categories\":[\"social-networks\",\"twitter-settings\"]},\"18887\":{\"title\":\"Setting the Content Author for Facebook\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-content-author-for-facebook\\/\",\"categories\":[\"facebook-settings\",\"social-networks\"]},\"18888\":{\"title\":\"Setting the Content Publisher for Facebook\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-the-content-publisher-for-facebook\\/\",\"categories\":[\"facebook-settings\",\"social-networks\"]},\"18889\":{\"title\":\"How to Create a Google News Sitemap\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-create-a-google-news-sitemap\\/\",\"categories\":[\"google-news-sitemap\"]},\"18890\":{\"title\":\"Including Videos in Custom Fields in Your Video Sitemap\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/including-videos-in-custom-fields-in-your-video-sitemap\\/\",\"categories\":[\"video-sitemap\"]},\"18891\":{\"title\":\"What is a Dynamically Generated Sitemap and Why is it Better to Use?\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/what-is-a-dynamically-generated-sitemap-and-why-is-it-better-to-use\\/\",\"categories\":[\"frequently-asked-questions\",\"video-sitemap\",\"xml-sitemap\"]},\"18892\":{\"title\":\"How to Create a Video Sitemap\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-create-a-video-sitemap\\/\",\"categories\":[\"video-sitemap\"]},\"18893\":{\"title\":\"Adding Your Facebook Admin ID\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/adding-your-facebook-admin-id\\/\",\"categories\":[\"facebook-settings\",\"social-networks\"]},\"18894\":{\"title\":\"Adding Your Facebook App ID\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/adding-your-facebook-app-id\\/\",\"categories\":[\"facebook-settings\",\"social-networks\"]},\"18895\":{\"title\":\"Access Control Settings\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/access-control-settings\\/\",\"categories\":[\"access-control-settings\"]},\"18896\":{\"title\":\"Advanced Settings for Google Analytics\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/advanced-settings-for-google-analytics\\/\",\"categories\":[\"google-analytics\"]},\"18897\":{\"title\":\"Miscellaneous Site Verification\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/miscellaneous-site-verification\\/\",\"categories\":[\"webmaster-tools\",\"webmaster-verification\"]},\"18898\":{\"title\":\"Displaying Your Social Media Profiles in Knowledge Panel\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/displaying-your-social-media-profiles-in-knowledge-panel\\/\",\"categories\":[\"schema-settings\",\"social-networks\"]},\"18899\":{\"title\":\"How to Create an RSS Sitemap\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-create-an-rss-sitemap\\/\",\"categories\":[\"rss-sitemap\"]},\"18900\":{\"title\":\"Excluding Images from the XML Sitemap\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/excluding-images-from-the-xml-sitemap\\/\",\"categories\":[\"xml-sitemap\"]},\"18901\":{\"title\":\"Adding non-WordPress Content to the XML Sitemap\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/adding-non-wordpress-content-to-the-xml-sitemap\\/\",\"categories\":[\"xml-sitemap\"]},\"18903\":{\"title\":\"Setting a Default Image for Twitter\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-a-default-image-for-twitter\\/\",\"categories\":[\"social-networks\",\"twitter-settings\"]},\"18904\":{\"title\":\"Setting a Default Image for Facebook\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-a-default-image-for-facebook\\/\",\"categories\":[\"facebook-settings\",\"social-networks\"]},\"18905\":{\"title\":\"Setting a Title Separator\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-a-title-separator\\/\",\"categories\":[\"search-appearance\"]},\"18906\":{\"title\":\"How to Protect Your Content With RSS Content Settings\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-protect-your-content-with-rss-content-settings\\/\",\"categories\":[\"rss-content-settings\"]},\"18907\":{\"title\":\"How to Connect Your Site with Google Tag Manager\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-connect-your-site-with-google-tag-manager\\/\",\"categories\":[\"google-analytics\"]},\"18908\":{\"title\":\"How to Connect Your Site with Google Analytics\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-connect-your-site-with-google-analytics\\/\",\"categories\":[\"google-analytics\"]},\"18909\":{\"title\":\"How to Verify Your Site with Pinterest\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-verify-your-site-with-pinterest\\/\",\"categories\":[\"pinterest-settings\",\"social-networks\",\"webmaster-tools\",\"webmaster-verification\"]},\"18910\":{\"title\":\"How to Verify Your Site with Yandex Webmaster Tools\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-verify-your-site-with-yandex-webmaster-tools\\/\",\"categories\":[\"webmaster-tools\",\"webmaster-verification\"]},\"18911\":{\"title\":\"How to Verify Your Site with Bing Webmaster Tools\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-verify-your-site-with-bing-webmaster-tools\\/\",\"categories\":[\"bing-webmaster-tools\",\"webmaster-tools\",\"webmaster-verification\"]},\"18912\":{\"title\":\"How to Verify Your Site with Google Search Console\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-verify-your-site-with-google-search-console\\/\",\"categories\":[\"google-search-console\",\"webmaster-tools\",\"webmaster-verification\"]},\"18913\":{\"title\":\"Usage Tracking\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/usage-tracking\\/\",\"categories\":[\"frequently-asked-questions\"]},\"18915\":{\"title\":\"How do I use All in One SEO in my language?\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-do-i-use-all-in-one-seo-in-my-language\\/\",\"categories\":[\"frequently-asked-questions\"]},\"18920\":{\"title\":\"NGINX rewrite rules for Robots.txt\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/nginx-rewrite-rules-for-robots-txt\\/\",\"categories\":[\"robots-txt\"]},\"18927\":{\"title\":\"Supported PHP Versions for All in One SEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/supported-php-version\\/\",\"categories\":[\"troubleshooting\"]},\"18929\":{\"title\":\"Using a different CDN for script enqueuing\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/using-a-different-cdn-for-script-enqueuing\\/\",\"categories\":[\"troubleshooting\"]},\"18930\":{\"title\":\"How do I get Google to show sitelinks for my site?\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-do-i-get-google-to-show-sitelinks-for-my-site\\/\",\"categories\":[\"frequently-asked-questions\"]},\"18954\":{\"title\":\"How does the import process for SEO data work?\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-does-the-import-process-for-seo-data-work\\/\",\"categories\":[\"frequently-asked-questions\",\"importer-exporter\"]},\"18960\":{\"title\":\"Robots.txt Editor for Multisite Networks\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/robots-txt-editor-for-multisite-networks\\/\",\"categories\":[\"robots-txt\"]},\"18961\":{\"title\":\"What are the minimum requirements for All in One SEO?\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/what-are-the-minimum-requirements-for-all-in-one-seo-pack\\/\",\"categories\":[\"frequently-asked-questions\"]},\"18964\":{\"title\":\"How do I use your API code examples?\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-do-i-use-your-api-code-examples\\/\",\"categories\":[\"frequently-asked-questions\"]},\"18969\":{\"title\":\"XML Parsing Error - This page contains the following errors\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/this-page-contains-the-following-errors\\/\",\"categories\":[\"google-news-sitemap\",\"troubleshooting\",\"video-sitemap\",\"xml-sitemap\"]},\"18972\":{\"title\":\"The File Editor or Robots.txt modules are missing\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/the-file-editor-or-robots-txt-modules-are-missing\\/\",\"categories\":[\"frequently-asked-questions\"]},\"18977\":{\"title\":\"Excluding the XML Sitemap from caching\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/excluding-the-xml-sitemap-from-caching\\/\",\"categories\":[\"xml-sitemap\"]},\"18982\":{\"title\":\"Why doesn\'t the title and description I set appear in search results?\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/why-doesnt-the-title-and-description-i-set-appear-in-search-results\\/\",\"categories\":[\"frequently-asked-questions\",\"post-page-settings\"]},\"18983\":{\"title\":\"Can I remove the date from Google search results?\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/can-i-remove-the-date-from-google-search-results\\/\",\"categories\":[\"frequently-asked-questions\"]},\"18985\":{\"title\":\"Setting up HTTPS SSL\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/setting-up-https-ssl\\/\",\"categories\":[\"general-seo-topics\"]},\"18995\":{\"title\":\"How to Increase the WordPress PHP Memory Limit\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/increase-wordpress-php-memory-limit\\/\",\"categories\":[\"troubleshooting\"]},\"19002\":{\"title\":\"Checking Index Status in Google Search Results\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/checking-index-status-in-google-search-results\\/\",\"categories\":[\"general-seo-topics\"]},\"19006\":{\"title\":\"SEO Data Importer\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/seo-data-importer\\/\",\"categories\":[\"seo-data-importer\"]},\"19008\":{\"title\":\"How to troubleshoot issues with All in One SEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/how-to-troubleshoot-issues-with-all-in-one-seo-pack\\/\",\"categories\":[\"troubleshooting\"]},\"19010\":{\"title\":\"Quality Guidelines for SEO Titles and Descriptions\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/quality-guidelines-for-seo-titles-and-descriptions\\/\",\"categories\":[\"general-seo-topics\"]},\"19016\":{\"title\":\"Top Tips for Good On-Page SEO\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/top-tips-for-good-on-page-seo\\/\",\"categories\":[\"general-seo-topics\"]},\"19017\":{\"title\":\"Meta Descriptions\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/meta-descriptions\\/\",\"categories\":[\"general-seo-topics\"]},\"19028\":{\"title\":\"What is SEO meta?\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/what-is-seo-meta\\/\",\"categories\":[\"getting-started\"]},\"19029\":{\"title\":\"Social Meta Settings - Individual Page\\/Post Settings\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/social-meta-settings-individual-pagepost-settings\\/\",\"categories\":[\"post-page-settings\",\"social-networks\"]},\"19030\":{\"title\":\"File Editor Module\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/file-editor-module\\/\",\"categories\":[\"file-editor\"]},\"19031\":{\"title\":\"Social Meta Settings\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/social-meta-module\\/\",\"categories\":[\"social-networks\"]},\"19032\":{\"title\":\"Importer & Exporter Module\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/importer-exporter-module\\/\",\"categories\":[\"importer-exporter\"]},\"19033\":{\"title\":\"XML Sitemaps Module\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/xml-sitemaps-module\\/\",\"categories\":[\"xml-sitemap\"]},\"19034\":{\"title\":\"Feature Manager\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/feature-manager\\/\",\"categories\":[\"feature-manager\"]},\"19035\":{\"title\":\"Advanced Settings for All in One SEO Pack\",\"url\":\"https:\\/\\/aioseo.com\\/docs\\/all-in-one-seo-pack-advanced-settings\\/\",\"categories\":[\"advanced-settings\"]}}}', 'no'),
(372, '_aioseo_cache_expiration_admin_help_docs', '1619030475', 'no'),
(375, 'finished_updating_comment_type', '1', 'yes'),
(376, 'aioseo_options_localized', 'a:2:{s:33:\"searchAppearance_global_siteTitle\";s:60:\"#site_title#current_day #separator_sa #tagline&nbsp;#tagline\";s:39:\"searchAppearance_global_metaDescription\";s:30:\"#author_first_name #site_title\";}', 'yes');
INSERT INTO `wp_options` (`option_id`, `option_name`, `option_value`, `autoload`) VALUES
(377, 'aioseo_options', '{\"internal\":{\"searchAppearanceDynamicBackup\":{\"postTypes\":[],\"taxonomies\":[]},\"socialFacebookDynamicBackup\":{\"postTypes\":[],\"taxonomies\":[]}},\"webmasterTools\":{\"google\":null,\"bing\":null,\"yandex\":null,\"baidu\":null,\"pinterest\":null,\"alexa\":null,\"norton\":null,\"miscellaneousVerification\":null},\"breadcrumbs\":{\"enable\":null,\"separator\":\"&raquo;\",\"homepageLink\":true,\"homepageLabel\":\"Home\",\"breadcrumbPrefix\":null,\"archiveFormat\":\"Archives for %\",\"searchResultFormat\":\"Search for \'%\'\",\"errorFormat404\":\"404 Error: page not found\",\"showCurrentItem\":true,\"linkCurrentItem\":false},\"rssContent\":{\"before\":null,\"after\":\"&lt;p&gt;The post #post_link first appeared on #site_link.&lt;\\/p&gt;\"},\"advanced\":{\"truSeo\":true,\"seoAnalysis\":true,\"dashboardWidget\":true,\"announcements\":true,\"postTypes\":{\"all\":true,\"included\":[\"post\",\"page\",\"product\"]},\"taxonomies\":{\"all\":true,\"included\":[\"category\",\"post_tag\",\"product_cat\",\"product_tag\"]},\"uninstall\":false},\"sitemap\":{\"general\":{\"enable\":true,\"filename\":\"sitemap\",\"indexes\":true,\"linksPerIndex\":1000,\"postTypes\":{\"all\":true,\"included\":[\"post\",\"page\",\"attachment\",\"product\"]},\"taxonomies\":{\"all\":true,\"included\":[\"category\",\"post_tag\",\"product_cat\",\"product_tag\"]},\"author\":false,\"date\":false,\"additionalPages\":{\"enable\":false,\"pages\":[]},\"advancedSettings\":{\"enable\":false,\"excludeImages\":false,\"excludePosts\":[],\"excludeTerms\":[],\"priority\":{\"homePage\":{\"priority\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\",\"frequency\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\"},\"postTypes\":{\"grouped\":true,\"priority\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\",\"frequency\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\"},\"taxonomies\":{\"grouped\":true,\"priority\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\",\"frequency\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\"},\"archive\":{\"priority\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\",\"frequency\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\"},\"author\":{\"priority\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\",\"frequency\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\"}}}},\"rss\":{\"enable\":true,\"linksPerIndex\":50,\"postTypes\":{\"all\":true,\"included\":[\"post\",\"page\",\"product\"]}},\"dynamic\":{\"priority\":{\"postTypes\":{\"post\":{\"priority\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\",\"frequency\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\"},\"page\":{\"priority\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\",\"frequency\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\"},\"attachment\":{\"priority\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\",\"frequency\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\"}},\"taxonomies\":{\"category\":{\"priority\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\",\"frequency\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\"},\"post_tag\":{\"priority\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\",\"frequency\":\"{\\\"label\\\":\\\"default\\\",\\\"value\\\":\\\"default\\\"}\"}}}}},\"social\":{\"profiles\":{\"sameUsername\":{\"enable\":false,\"username\":null,\"included\":[\"facebookPageUrl\",\"twitterUrl\",\"pinterestUrl\",\"instagramUrl\",\"youtubeUrl\",\"linkedinUrl\"]},\"urls\":{\"facebookPageUrl\":null,\"twitterUrl\":null,\"instagramUrl\":null,\"pinterestUrl\":null,\"youtubeUrl\":null,\"linkedinUrl\":null,\"tumblrUrl\":null,\"yelpPageUrl\":null,\"soundCloudUrl\":null,\"wikipediaUrl\":null,\"myspaceUrl\":null,\"googlePlacesUrl\":null}},\"siteSocialProfiles\":null,\"facebook\":{\"general\":{\"enable\":true,\"defaultImageSourcePosts\":\"default\",\"customFieldImagePosts\":null,\"defaultImagePosts\":\"\",\"defaultImagePostsWidth\":\"\",\"defaultImagePostsHeight\":\"\",\"showAuthor\":true,\"siteName\":\"#site_title #separator_sa #tagline\",\"dynamic\":{\"postTypes\":{\"post\":{\"objectType\":\"article\"},\"page\":{\"objectType\":\"article\"},\"attachment\":{\"objectType\":\"article\"}},\"taxonomies\":{\"category\":{\"objectType\":\"article\"},\"post_tag\":{\"objectType\":\"article\"}},\"archives\":[]}},\"homePage\":{\"image\":\"\",\"title\":\"\",\"description\":\"\",\"imageWidth\":\"\",\"imageHeight\":\"\",\"objectType\":\"website\"},\"advanced\":{\"enable\":false,\"adminId\":\"\",\"appId\":\"\",\"authorUrl\":\"\",\"generateArticleTags\":false,\"useKeywordsInTags\":true,\"useCategoriesInTags\":true,\"usePostTagsInTags\":true}},\"twitter\":{\"general\":{\"enable\":true,\"defaultCardType\":\"summary\",\"defaultImageSourcePosts\":\"default\",\"customFieldImagePosts\":null,\"defaultImagePosts\":\"\",\"showAuthor\":true,\"additionalData\":false},\"homePage\":{\"image\":\"\",\"title\":\"\",\"description\":\"\",\"cardType\":\"summary\"}}},\"searchAppearance\":{\"global\":{\"separator\":\"&#45;\",\"siteTitle\":\"#site_title#current_day #separator_sa #tagline&nbsp;#tagline\",\"metaDescription\":\"#author_first_name #site_title\",\"keywords\":null,\"schema\":{\"siteRepresents\":\"organization\",\"person\":null,\"organizationName\":\"BanjaraClothing\",\"organizationLogo\":\"\",\"personName\":null,\"personLogo\":null,\"phone\":\"+918764019502\",\"contactType\":\"Sales\",\"contactTypeManual\":null}},\"advanced\":{\"globalRobotsMeta\":{\"default\":true,\"noindex\":false,\"nofollow\":false,\"noindexPaginated\":true,\"nofollowPaginated\":true,\"noindexFeed\":true,\"noarchive\":false,\"noimageindex\":false,\"notranslate\":false,\"nosnippet\":false,\"noodp\":false,\"maxSnippet\":-1,\"maxVideoPreview\":-1,\"maxImagePreview\":\"large\"},\"sitelinks\":true,\"noIndexEmptyCat\":true,\"removeStopWords\":false,\"noPaginationForCanonical\":true,\"useKeywords\":false,\"keywordsLooking\":true,\"useCategoriesForMetaKeywords\":false,\"useTagsForMetaKeywords\":false,\"dynamicallyGenerateKeywords\":false,\"pagedFormat\":\"- Page #page_number\"},\"archives\":{\"author\":{\"show\":true,\"title\":\"#author_name #separator_sa #site_title\",\"metaDescription\":\"#author_bio\",\"advanced\":{\"robotsMeta\":{\"default\":true,\"noindex\":false,\"nofollow\":false,\"noarchive\":false,\"noimageindex\":false,\"notranslate\":false,\"nosnippet\":false,\"noodp\":false,\"maxSnippet\":-1,\"maxVideoPreview\":-1,\"maxImagePreview\":\"large\"},\"showDateInGooglePreview\":true,\"showPostThumbnailInSearch\":true,\"showMetaBox\":true,\"keywords\":null}},\"date\":{\"show\":true,\"title\":\"#archive_date #separator_sa #site_title\",\"metaDescription\":\"\",\"advanced\":{\"robotsMeta\":{\"default\":true,\"noindex\":false,\"nofollow\":false,\"noarchive\":false,\"noimageindex\":false,\"notranslate\":false,\"nosnippet\":false,\"noodp\":false,\"maxSnippet\":-1,\"maxVideoPreview\":-1,\"maxImagePreview\":\"large\"},\"showDateInGooglePreview\":true,\"showPostThumbnailInSearch\":true,\"showMetaBox\":true,\"keywords\":null}},\"search\":{\"show\":false,\"title\":\"#search_term #separator_sa #site_title\",\"metaDescription\":\"\",\"advanced\":{\"robotsMeta\":{\"default\":false,\"noindex\":true,\"nofollow\":false,\"noarchive\":false,\"noimageindex\":false,\"notranslate\":false,\"nosnippet\":false,\"noodp\":false,\"maxSnippet\":-1,\"maxVideoPreview\":-1,\"maxImagePreview\":\"large\"},\"showDateInGooglePreview\":true,\"showPostThumbnailInSearch\":true,\"showMetaBox\":true,\"keywords\":null}}},\"dynamic\":{\"postTypes\":{\"post\":{\"show\":true,\"title\":\"#post_title #separator_sa #site_title\",\"metaDescription\":\"#post_excerpt\",\"schemaType\":\"Article\",\"webPageType\":\"WebPage\",\"articleType\":\"BlogPosting\",\"customFields\":null,\"advanced\":{\"robotsMeta\":{\"default\":true,\"noindex\":false,\"nofollow\":false,\"noarchive\":false,\"noimageindex\":false,\"notranslate\":false,\"nosnippet\":false,\"noodp\":false,\"maxSnippet\":-1,\"maxVideoPreview\":-1,\"maxImagePreview\":\"large\"},\"bulkEditing\":\"enabled\",\"showDateInGooglePreview\":true,\"showPostThumbnailInSearch\":true,\"showMetaBox\":true}},\"page\":{\"show\":true,\"title\":\"#post_title #separator_sa #site_title\",\"metaDescription\":\"#post_content\",\"schemaType\":\"WebPage\",\"webPageType\":\"WebPage\",\"articleType\":\"BlogPosting\",\"customFields\":null,\"advanced\":{\"robotsMeta\":{\"default\":true,\"noindex\":false,\"nofollow\":false,\"noarchive\":false,\"noimageindex\":false,\"notranslate\":false,\"nosnippet\":false,\"noodp\":false,\"maxSnippet\":-1,\"maxVideoPreview\":-1,\"maxImagePreview\":\"large\"},\"bulkEditing\":\"enabled\",\"showDateInGooglePreview\":true,\"showPostThumbnailInSearch\":true,\"showMetaBox\":true}},\"attachment\":{\"show\":true,\"title\":\"#post_title #separator_sa #site_title\",\"metaDescription\":\"#attachment_caption\",\"schemaType\":\"ItemPage\",\"webPageType\":\"ItemPage\",\"articleType\":\"BlogPosting\",\"customFields\":null,\"advanced\":{\"robotsMeta\":{\"default\":true,\"noindex\":false,\"nofollow\":false,\"noarchive\":false,\"noimageindex\":false,\"notranslate\":false,\"nosnippet\":false,\"noodp\":false,\"maxSnippet\":-1,\"maxVideoPreview\":-1,\"maxImagePreview\":\"large\"},\"bulkEditing\":\"enabled\",\"showDateInGooglePreview\":true,\"showPostThumbnailInSearch\":true,\"showMetaBox\":true},\"redirectAttachmentUrls\":\"attachment\"}},\"taxonomies\":{\"category\":{\"show\":true,\"title\":\"#taxonomy_title #separator_sa #site_title\",\"metaDescription\":\"#taxonomy_description\",\"advanced\":{\"robotsMeta\":{\"default\":true,\"noindex\":false,\"nofollow\":false,\"noarchive\":false,\"noimageindex\":false,\"notranslate\":false,\"nosnippet\":false,\"noodp\":false,\"maxSnippet\":-1,\"maxVideoPreview\":-1,\"maxImagePreview\":\"large\"},\"showDateInGooglePreview\":true,\"showPostThumbnailInSearch\":true,\"showMetaBox\":true}},\"post_tag\":{\"show\":true,\"title\":\"#taxonomy_title #separator_sa #site_title\",\"metaDescription\":\"#taxonomy_description\",\"advanced\":{\"robotsMeta\":{\"default\":true,\"noindex\":false,\"nofollow\":false,\"noarchive\":false,\"noimageindex\":false,\"notranslate\":false,\"nosnippet\":false,\"noodp\":false,\"maxSnippet\":-1,\"maxVideoPreview\":-1,\"maxImagePreview\":\"large\"},\"showDateInGooglePreview\":true,\"showPostThumbnailInSearch\":true,\"showMetaBox\":true}}},\"archives\":[]}},\"tools\":{\"robots\":{\"enable\":false,\"rules\":[],\"robotsDetected\":true},\"importExport\":{\"backup\":{\"lastTime\":null,\"data\":null}}},\"deprecated\":{\"webmasterTools\":{\"googleAnalytics\":{\"id\":null,\"advanced\":false,\"trackingDomain\":null,\"multipleDomains\":false,\"additionalDomains\":null,\"anonymizeIp\":false,\"displayAdvertiserTracking\":false,\"excludeUsers\":[],\"trackOutboundLinks\":false,\"enhancedLinkAttribution\":false,\"enhancedEcommerce\":false}},\"searchAppearance\":{\"global\":{\"descriptionFormat\":null,\"schema\":{\"enableSchemaMarkup\":true}},\"advanced\":{\"autogenerateDescriptions\":true,\"runShortcodesInDescription\":true,\"useContentForAutogeneratedDescriptions\":false,\"excludePosts\":[],\"excludeTerms\":[]}},\"sitemap\":{\"general\":{\"advancedSettings\":{\"dynamic\":true}}},\"tools\":{\"blocker\":{\"blockBots\":null,\"blockReferer\":null,\"track\":null,\"custom\":{\"enable\":null,\"bots\":\"Abonti\\naggregator\\nAhrefsBot\\nasterias\\nBDCbot\\nBLEXBot\\nBuiltBotTough\\nBullseye\\nBunnySlippers\\nca-crawler\\nCCBot\\nCegbfeieh\\nCheeseBot\\nCherryPicker\\nCopyRightCheck\\ncosmos\\nCrescent\\ndiscobot\\nDittoSpyder\\nDotBot\\nDownload Ninja\\nEasouSpider\\nEmailCollector\\nEmailSiphon\\nEmailWolf\\nEroCrawler\\nExtractorPro\\nFasterfox\\nFeedBooster\\nFoobot\\nGenieo\\ngrub-client\\nHarvest\\nhloader\\nhttplib\\nHTTrack\\nhumanlinks\\nieautodiscovery\\nInfoNaviRobot\\nIstellaBot\\nJava\\/1.\\nJennyBot\\nk2spider\\nKenjin Spider\\nKeyword Density\\/0.9\\nlarbin\\nLexiBot\\nlibWeb\\nlibwww\\nLinkextractorPro\\nlinko\\nLinkScan\\/8.1a Unix\\nLinkWalker\\nLNSpiderguy\\nlwp-trivial\\nmagpie\\nMata Hari\\nMaxPointCrawler\\nMegaIndex\\nMicrosoft URL Control\\nMIIxpc\\nMippin\\nMissigua Locator\\nMister PiX\\nMJ12bot\\nmoget\\nMSIECrawler\\nNetAnts\\nNICErsPRO\\nNiki-Bot\\nNPBot\\nNutch\\nOffline Explorer\\nOpenfind\\npanscient.com\\nPHP\\/5.{\\nProPowerBot\\/2.14\\nProWebWalker\\nPython-urllib\\nQueryN Metasearch\\nRepoMonkey\\nSISTRIX\\nsitecheck.Internetseer.com\\nSiteSnagger\\nSnapPreviewBot\\nSogou\\nSpankBot\\nspanner\\nspbot\\nSpinn3r\\nsuzuran\\nSzukacz\\/1.4\\nTeleport\\nTelesoft\\nThe Intraformant\\nTheNomad\\nTightTwatBot\\nTitan\\ntoCrawl\\/UrlDispatcher\\nTrue_Robot\\nturingos\\nTurnitinBot\\nUbiCrawler\\nUnisterBot\\nURLy Warning\\nVCI\\nWBSearchBot\\nWeb Downloader\\/6.9\\nWeb Image Collector\\nWebAuto\\nWebBandit\\nWebCopier\\nWebEnhancer\\nWebmasterWorldForumBot\\nWebReaper\\nWebSauger\\nWebsite Quester\\nWebster Pro\\nWebStripper\\nWebZip\\nWotbox\\nwsr-agent\\nWWW-Collector-E\\nXenu\\nZao\\nZeus\\nZyBORG\\ncoccoc\\nIncutio\\nlmspider\\nmemoryBot\\nserf\\nUnknown\\nuptime files\",\"referer\":\"semalt.com\\nkambasoft.com\\nsavetubevideo.com\\nbuttons-for-website.com\\nsharebutton.net\\nsoundfrost.org\\nsrecorder.com\\nsoftomix.com\\nsoftomix.net\\nmyprintscreen.com\\njoinandplay.me\\nfbfreegifts.com\\nopenmediasoft.com\\nzazagames.org\\nextener.org\\nopenfrost.com\\nopenfrost.net\\ngooglsucks.com\\nbest-seo-offer.com\\nbuttons-for-your-website.com\\nwww.Get-Free-Traffic-Now.com\\nbest-seo-solution.com\\nbuy-cheap-online.info\\nsite3.free-share-buttons.com\\nwebmaster-traffic.com\"}}}}}', 'yes'),
(378, 'aioseo_options_lite', '{\"advanced\":{\"usageTracking\":true}}', 'yes'),
(382, 'litespeed.gui._summary', 'a:2:{s:11:\"new_version\";i:1618512546;s:5:\"score\";i:1618858146;}', 'yes'),
(383, '_transient_wpforms_htaccess_file', 'a:3:{s:4:\"size\";i:737;s:5:\"mtime\";i:1618426147;s:5:\"ctime\";i:1618426147;}', 'yes'),
(384, 'wpforms_notifications', 'a:4:{s:6:\"update\";i:1618426147;s:4:\"feed\";a:0:{}s:6:\"events\";a:0:{}s:9:\"dismissed\";a:0:{}}', 'yes'),
(385, 'om_notifications', 'a:4:{s:7:\"updated\";i:1618426149;s:4:\"feed\";a:0:{}s:6:\"events\";a:0:{}s:9:\"dismissed\";a:0:{}}', 'no'),
(386, 'can_compress_scripts', '1', 'no'),
(387, 'monsterinsights_notifications_run', 'a:14:{s:37:\"monsterinsights_notification_visitors\";i:1618426149;s:37:\"monsterinsights_notification_audience\";i:1618426149;s:42:\"monsterinsights_notification_mobile_device\";i:1618426149;s:43:\"monsterinsights_notification_upgrade_to_pro\";i:1618426149;s:40:\"monsterinsights_notification_bounce_rate\";i:1618426149;s:47:\"monsterinsights_notification_returning_visitors\";i:1618426149;s:45:\"monsterinsights_notification_traffic_dropping\";i:1618426149;s:62:\"monsterinsights_notification_upgrade_for_search_console_report\";i:1618426149;s:56:\"monsterinsights_notification_upgrade_for_form_conversion\";i:1618426149;s:56:\"monsterinsights_notification_upgrade_for_email_summaries\";i:1618426149;s:56:\"monsterinsights_notification_upgrade_for_google_optimize\";i:1618426149;s:56:\"monsterinsights_notification_to_add_more_file_extensions\";i:1618426149;s:53:\"monsterinsights_notification_to_setup_affiliate_links\";i:1618426149;s:46:\"monsterinsights_notification_headline_analyzer\";i:1618426149;}', 'yes'),
(389, '_aioseo_cache_analyze_site_code', 'a:1:{s:27:\"https://banjaraclothing.com\";i:200;}', 'no'),
(390, '_aioseo_cache_expiration_analyze_site_code', '1618426786', 'no'),
(391, '_aioseo_cache_analyze_site_body', 'a:1:{s:27:\"https://banjaraclothing.com\";O:8:\"stdClass\":4:{s:7:\"success\";b:1;s:7:\"results\";O:8:\"stdClass\":4:{s:5:\"basic\";O:8:\"stdClass\":8:{s:5:\"title\";O:8:\"stdClass\":3:{s:6:\"status\";s:6:\"passed\";s:5:\"value\";s:47:\"banjaraclothing – Just another WordPress site\";s:6:\"length\";i:45;}s:11:\"description\";O:8:\"stdClass\":4:{s:6:\"status\";s:5:\"error\";s:5:\"value\";s:0:\"\";s:6:\"length\";i:0;s:5:\"error\";s:19:\"description-missing\";}s:8:\"keywords\";O:8:\"stdClass\":10:{s:6:\"search\";i:5;s:9:\"wordpress\";i:5;s:5:\"world\";i:4;s:4:\"post\";i:3;s:4:\"june\";i:2;s:10:\"categories\";i:2;s:8:\"comments\";i:2;s:4:\"feed\";i:2;s:6:\"recent\";i:2;s:13:\"uncategorized\";i:2;}s:26:\"keywordsInTitleDescription\";O:8:\"stdClass\":3:{s:6:\"status\";s:5:\"error\";s:5:\"value\";O:8:\"stdClass\":2:{s:5:\"title\";O:8:\"stdClass\":1:{s:9:\"wordpress\";i:1;}s:11:\"description\";a:0:{}}s:5:\"error\";s:19:\"description-missing\";}s:6:\"h1Tags\";O:8:\"stdClass\":2:{s:6:\"status\";s:6:\"passed\";s:5:\"value\";a:1:{i:0;s:15:\"banjaraclothing\";}}s:6:\"h2Tags\";O:8:\"stdClass\":2:{s:6:\"status\";s:6:\"passed\";s:5:\"value\";a:6:{i:0;s:12:\"Hello world!\";i:1;s:12:\"Recent Posts\";i:2;s:15:\"Recent Comments\";i:3;s:8:\"Archives\";i:4;s:10:\"Categories\";i:5;s:4:\"Meta\";}}s:12:\"noImgAltAtts\";O:8:\"stdClass\":2:{s:6:\"status\";s:6:\"passed\";s:5:\"value\";a:0:{}}s:10:\"linksRatio\";O:8:\"stdClass\":2:{s:6:\"status\";s:6:\"passed\";s:5:\"value\";O:8:\"stdClass\":2:{s:8:\"internal\";i:13;s:8:\"external\";i:1;}}}s:8:\"advanced\";O:8:\"stdClass\":9:{s:13:\"searchPreview\";s:215:\"<div class=\"aioseo-google-search-preview\"><div class=\"domain\">https://banjaraclothing.com/</div><div class=\"site-title\">banjaraclothing – Just another WordPress site</div><div class=\"meta-description\"></div></div>\";s:19:\"mobileSearchPreview\";s:222:\"<div class=\"aioseo-google-search-preview mobile\"><div class=\"domain\">https://banjaraclothing.com/</div><div class=\"site-title\">banjaraclothing – Just another WordPress site</div><div class=\"meta-description\"></div></div>\";s:14:\"mobileSnapshot\";s:79:\"https://image.thum.io/get/auth/11036-aioseo/iphone6/https://banjaraclothing.com\";s:12:\"canonicalTag\";O:8:\"stdClass\":3:{s:6:\"status\";s:7:\"warning\";s:5:\"value\";s:0:\"\";s:5:\"error\";s:17:\"canonical-missing\";}s:7:\"noindex\";O:8:\"stdClass\":1:{s:6:\"status\";s:6:\"passed\";}s:19:\"wwwCanonicalization\";O:8:\"stdClass\":1:{s:6:\"status\";s:6:\"passed\";}s:11:\"robotsRules\";O:8:\"stdClass\":2:{s:6:\"status\";s:6:\"passed\";s:5:\"value\";s:170:\"User-agent: *\r\nAllow: /wp-admin/admin-ajax.php\r\nDisallow: /wp-admin/\r\n\r\nSitemap: https://banjaraclothing.com/sitemap.xml\r\nSitemap: https://banjaraclothing.com/sitemap.rss\";}s:9:\"openGraph\";O:8:\"stdClass\":3:{s:6:\"status\";s:5:\"error\";s:5:\"error\";s:11:\"ogp-missing\";s:5:\"value\";a:4:{i:0;s:8:\"og:title\";i:1;s:7:\"og:type\";i:2;s:8:\"og:image\";i:3;s:6:\"og:url\";}}s:6:\"schema\";O:8:\"stdClass\":2:{s:6:\"status\";s:5:\"error\";s:5:\"error\";s:14:\"schema-missing\";}}s:11:\"performance\";O:8:\"stdClass\":6:{s:13:\"hasImgExpires\";O:8:\"stdClass\":1:{s:6:\"status\";s:6:\"passed\";}s:12:\"unminifiedJs\";O:8:\"stdClass\":3:{s:6:\"status\";s:5:\"error\";s:5:\"value\";a:1:{i:0;s:77:\"https://banjaraclothing.com/wp-content/themes/twentytwenty/assets/js/index.js\";}s:5:\"error\";s:13:\"js-unminified\";}s:13:\"unminifiedCss\";O:8:\"stdClass\":3:{s:6:\"status\";s:5:\"error\";s:5:\"value\";a:2:{i:0;s:68:\"https://banjaraclothing.com/wp-content/themes/twentytwenty/style.css\";i:1;s:68:\"https://banjaraclothing.com/wp-content/themes/twentytwenty/style.css\";}s:5:\"error\";s:14:\"css-unminified\";}s:11:\"pageObjects\";O:8:\"stdClass\":3:{s:6:\"status\";s:6:\"passed\";s:5:\"value\";O:8:\"stdClass\":3:{s:6:\"images\";i:0;s:2:\"js\";i:3;s:3:\"css\";i:3;}s:5:\"total\";i:6;}s:8:\"pageSize\";O:8:\"stdClass\":2:{s:6:\"status\";s:6:\"passed\";s:5:\"value\";i:9060;}s:12:\"responseTime\";O:8:\"stdClass\":2:{s:6:\"status\";s:6:\"passed\";s:5:\"value\";d:0.040000000000000000832667268468867405317723751068115234375;}}s:8:\"security\";O:8:\"stdClass\":5:{s:14:\"visiblePlugins\";O:8:\"stdClass\":2:{s:6:\"status\";s:6:\"passed\";s:5:\"value\";a:0:{}}s:13:\"visibleThemes\";O:8:\"stdClass\":3:{s:6:\"status\";s:7:\"warning\";s:5:\"value\";a:1:{i:0;s:12:\"Twentytwenty\";}s:5:\"error\";s:14:\"themes-visible\";}s:16:\"directoryListing\";O:8:\"stdClass\":2:{s:6:\"status\";s:5:\"error\";s:5:\"error\";s:22:\"directory-listing-open\";}s:18:\"googleSafeBrowsing\";O:8:\"stdClass\":1:{s:6:\"status\";s:6:\"passed\";}s:16:\"secureConnection\";O:8:\"stdClass\":1:{s:6:\"status\";s:6:\"passed\";}}}s:5:\"tests\";i:22;s:5:\"score\";i:67;}}', 'no'),
(392, '_aioseo_cache_expiration_analyze_site_body', '1618426786', 'no'),
(398, 'wpforms_review', 'a:2:{s:4:\"time\";i:1618426240;s:9:\"dismissed\";b:0;}', 'yes'),
(399, 'monsterinsights_review', 'a:2:{s:4:\"time\";i:1618426240;s:9:\"dismissed\";b:0;}', 'yes'),
(404, '_aioseo_cache_rss_feed', 'a:4:{i:0;a:4:{s:3:\"url\";s:166:\"https://aioseo.com/how-to-integrate-social-media-into-your-website/?utm_source=rss&amp;utm_medium=rss&amp;utm_campaign=how-to-integrate-social-media-into-your-website\";s:5:\"title\";s:63:\"How to Integrate Social Media Into Your Website (5 Simple Ways)\";s:4:\"date\";s:13:\"Apr 13th 2021\";s:7:\"content\";s:131:\"Did you know that social media drives 31.24% of traffic to websites? That’s why it’s important to integrate social media int...\";}i:1;a:4:{s:3:\"url\";s:108:\"https://aioseo.com/seo-best-practices/?utm_source=rss&amp;utm_medium=rss&amp;utm_campaign=seo-best-practices\";s:5:\"title\";s:58:\"SEO Best Practices: 20 Ultimate SEO Tips to Boost Rankings\";s:4:\"date\";s:12:\"Apr 8th 2021\";s:7:\"content\";s:131:\"Research shows that 75% of searchers don’t bother clicking past the results on the first page, and the first 5 results for a g...\";}i:2;a:4:{s:3:\"url\";s:134:\"https://aioseo.com/introducing-redirection-manager/?utm_source=rss&amp;utm_medium=rss&amp;utm_campaign=introducing-redirection-manager\";s:5:\"title\";s:65:\"Introducing Redirection Manager: Easy Management of 301 Redirects\";s:4:\"date\";s:12:\"Apr 6th 2021\";s:7:\"content\";s:131:\"At All in One SEO (AIOSEO), we’ve always been listening to your feedback to make it the best SEO plugin for WordPress.\n\n\n\nWe k...\";}i:3;a:4:{s:3:\"url\";s:156:\"https://aioseo.com/migrating-from-rank-math-to-all-in-one-seo/?utm_source=rss&amp;utm_medium=rss&amp;utm_campaign=migrating-from-rank-math-to-all-in-one-seo\";s:5:\"title\";s:59:\"Migrating From Rank Math to All in One SEO (6 Simple Steps)\";s:4:\"date\";s:12:\"Apr 1st 2021\";s:7:\"content\";s:131:\"Do you want to know how to migrate your SEO settings from Rank Math to All in One SEO? It’s super easy!\n\n\n\nBut before anything...\";}}', 'no'),
(405, '_aioseo_cache_expiration_rss_feed', '1618469441', 'no'),
(445, 'ai1wm_updater', 'a:0:{}', 'yes'),
(449, '_site_transient_update_core', 'O:8:\"stdClass\":4:{s:7:\"updates\";a:1:{i:0;O:8:\"stdClass\":10:{s:8:\"response\";s:6:\"latest\";s:8:\"download\";s:59:\"https://downloads.wordpress.org/release/wordpress-5.7.1.zip\";s:6:\"locale\";s:5:\"en_US\";s:8:\"packages\";O:8:\"stdClass\":5:{s:4:\"full\";s:59:\"https://downloads.wordpress.org/release/wordpress-5.7.1.zip\";s:10:\"no_content\";s:70:\"https://downloads.wordpress.org/release/wordpress-5.7.1-no-content.zip\";s:11:\"new_bundled\";s:71:\"https://downloads.wordpress.org/release/wordpress-5.7.1-new-bundled.zip\";s:7:\"partial\";s:0:\"\";s:8:\"rollback\";s:0:\"\";}s:7:\"current\";s:5:\"5.7.1\";s:7:\"version\";s:5:\"5.7.1\";s:11:\"php_version\";s:6:\"5.6.20\";s:13:\"mysql_version\";s:3:\"5.0\";s:11:\"new_bundled\";s:3:\"5.6\";s:15:\"partial_version\";s:0:\"\";}}s:12:\"last_checked\";i:1619689969;s:15:\"version_checked\";s:5:\"5.7.1\";s:12:\"translations\";a:0:{}}', 'no'),
(452, 'auto_core_update_notified', 'a:4:{s:4:\"type\";s:7:\"success\";s:5:\"email\";s:21:\"hingerharsh@gmail.com\";s:7:\"version\";s:5:\"5.7.1\";s:9:\"timestamp\";i:1618477020;}', 'no'),
(464, '_transient_health-check-site-status-result', '{\"good\":18,\"recommended\":6,\"critical\":0}', 'yes'),
(490, 'wpforms_email_summaries_fetch_info_blocks_last_run', '1619279026', 'yes'),
(628, '_site_transient_timeout_php_check_6a93f292d9a273c004fc36e1f86d97b3', '1619737001', 'no'),
(629, '_site_transient_php_check_6a93f292d9a273c004fc36e1f86d97b3', 'a:5:{s:19:\"recommended_version\";s:3:\"7.4\";s:15:\"minimum_version\";s:6:\"5.6.20\";s:12:\"is_supported\";b:0;s:9:\"is_secure\";b:1;s:13:\"is_acceptable\";b:1;}', 'no'),
(943, '_transient_timeout__omapi_validate', '1619737772', 'no'),
(944, '_transient__omapi_validate', '1', 'no'),
(951, '_site_transient_timeout_theme_roots', '1619691770', 'no'),
(952, '_site_transient_theme_roots', 'a:5:{s:5:\"astra\";s:7:\"/themes\";s:14:\"twentynineteen\";s:7:\"/themes\";s:15:\"twentyseventeen\";s:7:\"/themes\";s:12:\"twentytwenty\";s:7:\"/themes\";s:15:\"twentytwentyone\";s:7:\"/themes\";}', 'no'),
(953, '_site_transient_update_themes', 'O:8:\"stdClass\":5:{s:12:\"last_checked\";i:1619689970;s:7:\"checked\";a:5:{s:5:\"astra\";s:5:\"2.4.3\";s:14:\"twentynineteen\";s:3:\"2.0\";s:15:\"twentyseventeen\";s:3:\"2.3\";s:12:\"twentytwenty\";s:3:\"1.7\";s:15:\"twentytwentyone\";s:3:\"1.2\";}s:8:\"response\";a:3:{s:5:\"astra\";a:6:{s:5:\"theme\";s:5:\"astra\";s:11:\"new_version\";s:5:\"3.3.3\";s:3:\"url\";s:35:\"https://wordpress.org/themes/astra/\";s:7:\"package\";s:53:\"https://downloads.wordpress.org/theme/astra.3.3.3.zip\";s:8:\"requires\";s:3:\"5.3\";s:12:\"requires_php\";s:3:\"5.3\";}s:15:\"twentyseventeen\";a:6:{s:5:\"theme\";s:15:\"twentyseventeen\";s:11:\"new_version\";s:3:\"2.7\";s:3:\"url\";s:45:\"https://wordpress.org/themes/twentyseventeen/\";s:7:\"package\";s:61:\"https://downloads.wordpress.org/theme/twentyseventeen.2.7.zip\";s:8:\"requires\";s:3:\"4.7\";s:12:\"requires_php\";s:5:\"5.2.4\";}s:15:\"twentytwentyone\";a:6:{s:5:\"theme\";s:15:\"twentytwentyone\";s:11:\"new_version\";s:3:\"1.3\";s:3:\"url\";s:45:\"https://wordpress.org/themes/twentytwentyone/\";s:7:\"package\";s:61:\"https://downloads.wordpress.org/theme/twentytwentyone.1.3.zip\";s:8:\"requires\";s:3:\"5.3\";s:12:\"requires_php\";s:3:\"5.6\";}}s:9:\"no_update\";a:2:{s:14:\"twentynineteen\";a:6:{s:5:\"theme\";s:14:\"twentynineteen\";s:11:\"new_version\";s:3:\"2.0\";s:3:\"url\";s:44:\"https://wordpress.org/themes/twentynineteen/\";s:7:\"package\";s:60:\"https://downloads.wordpress.org/theme/twentynineteen.2.0.zip\";s:8:\"requires\";s:5:\"4.9.6\";s:12:\"requires_php\";s:5:\"5.2.4\";}s:12:\"twentytwenty\";a:6:{s:5:\"theme\";s:12:\"twentytwenty\";s:11:\"new_version\";s:3:\"1.7\";s:3:\"url\";s:42:\"https://wordpress.org/themes/twentytwenty/\";s:7:\"package\";s:58:\"https://downloads.wordpress.org/theme/twentytwenty.1.7.zip\";s:8:\"requires\";s:3:\"4.7\";s:12:\"requires_php\";s:5:\"5.2.4\";}}s:12:\"translations\";a:0:{}}', 'no'),
(954, '_site_transient_update_plugins', 'O:8:\"stdClass\":5:{s:12:\"last_checked\";i:1619689970;s:7:\"checked\";a:12:{s:19:\"akismet/akismet.php\";s:5:\"4.1.9\";s:51:\"all-in-one-wp-migration/all-in-one-wp-migration.php\";s:4:\"7.40\";s:43:\"all-in-one-seo-pack/all_in_one_seo_pack.php\";s:7:\"4.1.0.1\";s:31:\"astra-widgets/astra-widgets.php\";s:5:\"1.2.3\";s:23:\"elementor/elementor.php\";s:5:\"2.9.8\";s:51:\"header-footer-elementor/header-footer-elementor.php\";s:5:\"1.4.1\";s:50:\"google-analytics-for-wordpress/googleanalytics.php\";s:6:\"7.17.0\";s:9:\"hello.php\";s:5:\"1.7.2\";s:35:\"litespeed-cache/litespeed-cache.php\";s:5:\"3.6.4\";s:37:\"optinmonster/optin-monster-wp-api.php\";s:5:\"2.3.1\";s:27:\"astra-sites/astra-sites.php\";s:5:\"2.2.4\";s:24:\"wpforms-lite/wpforms.php\";s:5:\"1.6.6\";}s:8:\"response\";a:6:{s:51:\"all-in-one-wp-migration/all-in-one-wp-migration.php\";O:8:\"stdClass\":12:{s:2:\"id\";s:37:\"w.org/plugins/all-in-one-wp-migration\";s:4:\"slug\";s:23:\"all-in-one-wp-migration\";s:6:\"plugin\";s:51:\"all-in-one-wp-migration/all-in-one-wp-migration.php\";s:11:\"new_version\";s:4:\"7.42\";s:3:\"url\";s:54:\"https://wordpress.org/plugins/all-in-one-wp-migration/\";s:7:\"package\";s:71:\"https://downloads.wordpress.org/plugin/all-in-one-wp-migration.7.42.zip\";s:5:\"icons\";a:2:{s:2:\"2x\";s:76:\"https://ps.w.org/all-in-one-wp-migration/assets/icon-256x256.png?rev=2458334\";s:2:\"1x\";s:76:\"https://ps.w.org/all-in-one-wp-migration/assets/icon-128x128.png?rev=2458334\";}s:7:\"banners\";a:2:{s:2:\"2x\";s:79:\"https://ps.w.org/all-in-one-wp-migration/assets/banner-1544x500.png?rev=2458349\";s:2:\"1x\";s:78:\"https://ps.w.org/all-in-one-wp-migration/assets/banner-772x250.png?rev=2458349\";}s:11:\"banners_rtl\";a:0:{}s:6:\"tested\";s:5:\"5.7.1\";s:12:\"requires_php\";s:6:\"5.2.17\";s:13:\"compatibility\";O:8:\"stdClass\":0:{}}s:43:\"all-in-one-seo-pack/all_in_one_seo_pack.php\";O:8:\"stdClass\":13:{s:2:\"id\";s:33:\"w.org/plugins/all-in-one-seo-pack\";s:4:\"slug\";s:19:\"all-in-one-seo-pack\";s:6:\"plugin\";s:43:\"all-in-one-seo-pack/all_in_one_seo_pack.php\";s:11:\"new_version\";s:7:\"4.1.0.2\";s:3:\"url\";s:50:\"https://wordpress.org/plugins/all-in-one-seo-pack/\";s:7:\"package\";s:70:\"https://downloads.wordpress.org/plugin/all-in-one-seo-pack.4.1.0.2.zip\";s:5:\"icons\";a:3:{s:2:\"2x\";s:72:\"https://ps.w.org/all-in-one-seo-pack/assets/icon-256x256.png?rev=2443290\";s:2:\"1x\";s:64:\"https://ps.w.org/all-in-one-seo-pack/assets/icon.svg?rev=2443290\";s:3:\"svg\";s:64:\"https://ps.w.org/all-in-one-seo-pack/assets/icon.svg?rev=2443290\";}s:7:\"banners\";a:2:{s:2:\"2x\";s:75:\"https://ps.w.org/all-in-one-seo-pack/assets/banner-1544x500.png?rev=2443290\";s:2:\"1x\";s:74:\"https://ps.w.org/all-in-one-seo-pack/assets/banner-772x250.png?rev=2443290\";}s:11:\"banners_rtl\";a:0:{}s:14:\"upgrade_notice\";s:56:\"<p>This update adds major improvements and bugfixes.</p>\";s:6:\"tested\";s:5:\"5.7.1\";s:12:\"requires_php\";s:3:\"5.4\";s:13:\"compatibility\";O:8:\"stdClass\":0:{}}s:31:\"astra-widgets/astra-widgets.php\";O:8:\"stdClass\":12:{s:2:\"id\";s:27:\"w.org/plugins/astra-widgets\";s:4:\"slug\";s:13:\"astra-widgets\";s:6:\"plugin\";s:31:\"astra-widgets/astra-widgets.php\";s:11:\"new_version\";s:5:\"1.2.8\";s:3:\"url\";s:44:\"https://wordpress.org/plugins/astra-widgets/\";s:7:\"package\";s:62:\"https://downloads.wordpress.org/plugin/astra-widgets.1.2.8.zip\";s:5:\"icons\";a:2:{s:2:\"1x\";s:58:\"https://ps.w.org/astra-widgets/assets/icon.svg?rev=2034708\";s:3:\"svg\";s:58:\"https://ps.w.org/astra-widgets/assets/icon.svg?rev=2034708\";}s:7:\"banners\";a:2:{s:2:\"2x\";s:69:\"https://ps.w.org/astra-widgets/assets/banner-1544x500.jpg?rev=2034708\";s:2:\"1x\";s:68:\"https://ps.w.org/astra-widgets/assets/banner-772x250.jpg?rev=2034708\";}s:11:\"banners_rtl\";a:0:{}s:6:\"tested\";s:5:\"5.7.1\";s:12:\"requires_php\";s:3:\"5.2\";s:13:\"compatibility\";O:8:\"stdClass\":0:{}}s:23:\"elementor/elementor.php\";O:8:\"stdClass\":12:{s:2:\"id\";s:23:\"w.org/plugins/elementor\";s:4:\"slug\";s:9:\"elementor\";s:6:\"plugin\";s:23:\"elementor/elementor.php\";s:11:\"new_version\";s:5:\"3.2.2\";s:3:\"url\";s:40:\"https://wordpress.org/plugins/elementor/\";s:7:\"package\";s:58:\"https://downloads.wordpress.org/plugin/elementor.3.2.2.zip\";s:5:\"icons\";a:3:{s:2:\"2x\";s:62:\"https://ps.w.org/elementor/assets/icon-256x256.png?rev=1427768\";s:2:\"1x\";s:54:\"https://ps.w.org/elementor/assets/icon.svg?rev=1426809\";s:3:\"svg\";s:54:\"https://ps.w.org/elementor/assets/icon.svg?rev=1426809\";}s:7:\"banners\";a:2:{s:2:\"2x\";s:65:\"https://ps.w.org/elementor/assets/banner-1544x500.png?rev=1475479\";s:2:\"1x\";s:64:\"https://ps.w.org/elementor/assets/banner-772x250.png?rev=1475479\";}s:11:\"banners_rtl\";a:0:{}s:6:\"tested\";s:5:\"5.7.1\";s:12:\"requires_php\";s:3:\"5.6\";s:13:\"compatibility\";O:8:\"stdClass\":0:{}}s:51:\"header-footer-elementor/header-footer-elementor.php\";O:8:\"stdClass\":12:{s:2:\"id\";s:37:\"w.org/plugins/header-footer-elementor\";s:4:\"slug\";s:23:\"header-footer-elementor\";s:6:\"plugin\";s:51:\"header-footer-elementor/header-footer-elementor.php\";s:11:\"new_version\";s:5:\"1.5.9\";s:3:\"url\";s:54:\"https://wordpress.org/plugins/header-footer-elementor/\";s:7:\"package\";s:72:\"https://downloads.wordpress.org/plugin/header-footer-elementor.1.5.9.zip\";s:5:\"icons\";a:2:{s:2:\"1x\";s:68:\"https://ps.w.org/header-footer-elementor/assets/icon.svg?rev=2308485\";s:3:\"svg\";s:68:\"https://ps.w.org/header-footer-elementor/assets/icon.svg?rev=2308485\";}s:7:\"banners\";a:2:{s:2:\"2x\";s:79:\"https://ps.w.org/header-footer-elementor/assets/banner-1544x500.jpg?rev=2308485\";s:2:\"1x\";s:78:\"https://ps.w.org/header-footer-elementor/assets/banner-772x250.jpg?rev=2308485\";}s:11:\"banners_rtl\";a:0:{}s:6:\"tested\";s:5:\"5.7.1\";s:12:\"requires_php\";s:3:\"5.4\";s:13:\"compatibility\";O:8:\"stdClass\":0:{}}s:27:\"astra-sites/astra-sites.php\";O:8:\"stdClass\":12:{s:2:\"id\";s:25:\"w.org/plugins/astra-sites\";s:4:\"slug\";s:11:\"astra-sites\";s:6:\"plugin\";s:27:\"astra-sites/astra-sites.php\";s:11:\"new_version\";s:5:\"2.6.2\";s:3:\"url\";s:42:\"https://wordpress.org/plugins/astra-sites/\";s:7:\"package\";s:60:\"https://downloads.wordpress.org/plugin/astra-sites.2.6.2.zip\";s:5:\"icons\";a:2:{s:2:\"1x\";s:56:\"https://ps.w.org/astra-sites/assets/icon.svg?rev=2345985\";s:3:\"svg\";s:56:\"https://ps.w.org/astra-sites/assets/icon.svg?rev=2345985\";}s:7:\"banners\";a:2:{s:2:\"2x\";s:67:\"https://ps.w.org/astra-sites/assets/banner-1544x500.jpg?rev=2370348\";s:2:\"1x\";s:66:\"https://ps.w.org/astra-sites/assets/banner-772x250.jpg?rev=2370348\";}s:11:\"banners_rtl\";a:0:{}s:6:\"tested\";s:5:\"5.7.1\";s:12:\"requires_php\";s:3:\"5.3\";s:13:\"compatibility\";O:8:\"stdClass\":0:{}}}s:12:\"translations\";a:0:{}s:9:\"no_update\";a:6:{s:19:\"akismet/akismet.php\";O:8:\"stdClass\":9:{s:2:\"id\";s:21:\"w.org/plugins/akismet\";s:4:\"slug\";s:7:\"akismet\";s:6:\"plugin\";s:19:\"akismet/akismet.php\";s:11:\"new_version\";s:5:\"4.1.9\";s:3:\"url\";s:38:\"https://wordpress.org/plugins/akismet/\";s:7:\"package\";s:56:\"https://downloads.wordpress.org/plugin/akismet.4.1.9.zip\";s:5:\"icons\";a:2:{s:2:\"2x\";s:59:\"https://ps.w.org/akismet/assets/icon-256x256.png?rev=969272\";s:2:\"1x\";s:59:\"https://ps.w.org/akismet/assets/icon-128x128.png?rev=969272\";}s:7:\"banners\";a:1:{s:2:\"1x\";s:61:\"https://ps.w.org/akismet/assets/banner-772x250.jpg?rev=479904\";}s:11:\"banners_rtl\";a:0:{}}s:50:\"google-analytics-for-wordpress/googleanalytics.php\";O:8:\"stdClass\":9:{s:2:\"id\";s:44:\"w.org/plugins/google-analytics-for-wordpress\";s:4:\"slug\";s:30:\"google-analytics-for-wordpress\";s:6:\"plugin\";s:50:\"google-analytics-for-wordpress/googleanalytics.php\";s:11:\"new_version\";s:6:\"7.17.0\";s:3:\"url\";s:61:\"https://wordpress.org/plugins/google-analytics-for-wordpress/\";s:7:\"package\";s:80:\"https://downloads.wordpress.org/plugin/google-analytics-for-wordpress.7.17.0.zip\";s:5:\"icons\";a:3:{s:2:\"2x\";s:83:\"https://ps.w.org/google-analytics-for-wordpress/assets/icon-256x256.png?rev=1598927\";s:2:\"1x\";s:75:\"https://ps.w.org/google-analytics-for-wordpress/assets/icon.svg?rev=1598927\";s:3:\"svg\";s:75:\"https://ps.w.org/google-analytics-for-wordpress/assets/icon.svg?rev=1598927\";}s:7:\"banners\";a:2:{s:2:\"2x\";s:86:\"https://ps.w.org/google-analytics-for-wordpress/assets/banner-1544x500.png?rev=2159532\";s:2:\"1x\";s:85:\"https://ps.w.org/google-analytics-for-wordpress/assets/banner-772x250.png?rev=2159532\";}s:11:\"banners_rtl\";a:0:{}}s:9:\"hello.php\";O:8:\"stdClass\":9:{s:2:\"id\";s:25:\"w.org/plugins/hello-dolly\";s:4:\"slug\";s:11:\"hello-dolly\";s:6:\"plugin\";s:9:\"hello.php\";s:11:\"new_version\";s:5:\"1.7.2\";s:3:\"url\";s:42:\"https://wordpress.org/plugins/hello-dolly/\";s:7:\"package\";s:60:\"https://downloads.wordpress.org/plugin/hello-dolly.1.7.2.zip\";s:5:\"icons\";a:2:{s:2:\"2x\";s:64:\"https://ps.w.org/hello-dolly/assets/icon-256x256.jpg?rev=2052855\";s:2:\"1x\";s:64:\"https://ps.w.org/hello-dolly/assets/icon-128x128.jpg?rev=2052855\";}s:7:\"banners\";a:1:{s:2:\"1x\";s:66:\"https://ps.w.org/hello-dolly/assets/banner-772x250.jpg?rev=2052855\";}s:11:\"banners_rtl\";a:0:{}}s:35:\"litespeed-cache/litespeed-cache.php\";O:8:\"stdClass\":9:{s:2:\"id\";s:29:\"w.org/plugins/litespeed-cache\";s:4:\"slug\";s:15:\"litespeed-cache\";s:6:\"plugin\";s:35:\"litespeed-cache/litespeed-cache.php\";s:11:\"new_version\";s:5:\"3.6.4\";s:3:\"url\";s:46:\"https://wordpress.org/plugins/litespeed-cache/\";s:7:\"package\";s:64:\"https://downloads.wordpress.org/plugin/litespeed-cache.3.6.4.zip\";s:5:\"icons\";a:2:{s:2:\"2x\";s:68:\"https://ps.w.org/litespeed-cache/assets/icon-256x256.png?rev=1574145\";s:2:\"1x\";s:68:\"https://ps.w.org/litespeed-cache/assets/icon-128x128.png?rev=1574145\";}s:7:\"banners\";a:2:{s:2:\"2x\";s:71:\"https://ps.w.org/litespeed-cache/assets/banner-1544x500.png?rev=2031698\";s:2:\"1x\";s:70:\"https://ps.w.org/litespeed-cache/assets/banner-772x250.png?rev=2031698\";}s:11:\"banners_rtl\";a:0:{}}s:37:\"optinmonster/optin-monster-wp-api.php\";O:8:\"stdClass\":9:{s:2:\"id\";s:26:\"w.org/plugins/optinmonster\";s:4:\"slug\";s:12:\"optinmonster\";s:6:\"plugin\";s:37:\"optinmonster/optin-monster-wp-api.php\";s:11:\"new_version\";s:5:\"2.3.1\";s:3:\"url\";s:43:\"https://wordpress.org/plugins/optinmonster/\";s:7:\"package\";s:55:\"https://downloads.wordpress.org/plugin/optinmonster.zip\";s:5:\"icons\";a:2:{s:2:\"2x\";s:65:\"https://ps.w.org/optinmonster/assets/icon-256x256.png?rev=1145864\";s:2:\"1x\";s:65:\"https://ps.w.org/optinmonster/assets/icon-128x128.png?rev=1145864\";}s:7:\"banners\";a:2:{s:2:\"2x\";s:68:\"https://ps.w.org/optinmonster/assets/banner-1544x500.png?rev=2311621\";s:2:\"1x\";s:67:\"https://ps.w.org/optinmonster/assets/banner-772x250.png?rev=2311621\";}s:11:\"banners_rtl\";a:0:{}}s:24:\"wpforms-lite/wpforms.php\";O:8:\"stdClass\":9:{s:2:\"id\";s:26:\"w.org/plugins/wpforms-lite\";s:4:\"slug\";s:12:\"wpforms-lite\";s:6:\"plugin\";s:24:\"wpforms-lite/wpforms.php\";s:11:\"new_version\";s:5:\"1.6.6\";s:3:\"url\";s:43:\"https://wordpress.org/plugins/wpforms-lite/\";s:7:\"package\";s:61:\"https://downloads.wordpress.org/plugin/wpforms-lite.1.6.6.zip\";s:5:\"icons\";a:2:{s:2:\"2x\";s:65:\"https://ps.w.org/wpforms-lite/assets/icon-256x256.png?rev=1371112\";s:2:\"1x\";s:65:\"https://ps.w.org/wpforms-lite/assets/icon-128x128.png?rev=1371112\";}s:7:\"banners\";a:2:{s:2:\"2x\";s:68:\"https://ps.w.org/wpforms-lite/assets/banner-1544x500.png?rev=1371112\";s:2:\"1x\";s:67:\"https://ps.w.org/wpforms-lite/assets/banner-772x250.png?rev=1371112\";}s:11:\"banners_rtl\";a:0:{}}}}', 'no');

-- --------------------------------------------------------

--
-- Table structure for table `wp_postmeta`
--

CREATE TABLE `wp_postmeta` (
  `meta_id` bigint(20) UNSIGNED NOT NULL,
  `post_id` bigint(20) UNSIGNED NOT NULL DEFAULT 0,
  `meta_key` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `meta_value` longtext COLLATE utf8mb4_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `wp_postmeta`
--

INSERT INTO `wp_postmeta` (`meta_id`, `post_id`, `meta_key`, `meta_value`) VALUES
(1, 2, '_wp_page_template', 'default'),
(2, 3, '_wp_page_template', 'default');

-- --------------------------------------------------------

--
-- Table structure for table `wp_posts`
--

CREATE TABLE `wp_posts` (
  `ID` bigint(20) UNSIGNED NOT NULL,
  `post_author` bigint(20) UNSIGNED NOT NULL DEFAULT 0,
  `post_date` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `post_date_gmt` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `post_content` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
  `post_title` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `post_excerpt` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `post_status` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'publish',
  `comment_status` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'open',
  `ping_status` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'open',
  `post_password` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `post_name` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `to_ping` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `pinged` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `post_modified` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `post_modified_gmt` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `post_content_filtered` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
  `post_parent` bigint(20) UNSIGNED NOT NULL DEFAULT 0,
  `guid` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `menu_order` int(11) NOT NULL DEFAULT 0,
  `post_type` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'post',
  `post_mime_type` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `comment_count` bigint(20) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `wp_posts`
--

INSERT INTO `wp_posts` (`ID`, `post_author`, `post_date`, `post_date_gmt`, `post_content`, `post_title`, `post_excerpt`, `post_status`, `comment_status`, `ping_status`, `post_password`, `post_name`, `to_ping`, `pinged`, `post_modified`, `post_modified_gmt`, `post_content_filtered`, `post_parent`, `guid`, `menu_order`, `post_type`, `post_mime_type`, `comment_count`) VALUES
(1, 1, '2021-04-14 18:38:27', '2021-04-14 18:38:27', '<!-- wp:paragraph -->\n<p>Welcome to WordPress. This is your first post. Edit or delete it, then start writing!</p>\n<!-- /wp:paragraph -->', 'Hello world!', '', 'publish', 'open', 'open', '', 'hello-world', '', '', '2021-04-14 18:38:27', '2021-04-14 18:38:27', '', 0, 'https://banjaraclothing.com/?p=1', 0, 'post', '', 1),
(2, 1, '2021-04-14 18:38:27', '2021-04-14 18:38:27', '<!-- wp:paragraph -->\n<p>This is an example page. It\'s different from a blog post because it will stay in one place and will show up in your site navigation (in most themes). Most people start with an About page that introduces them to potential site visitors. It might say something like this:</p>\n<!-- /wp:paragraph -->\n\n<!-- wp:quote -->\n<blockquote class=\"wp-block-quote\"><p>Hi there! I\'m a bike messenger by day, aspiring actor by night, and this is my website. I live in Los Angeles, have a great dog named Jack, and I like pi&#241;a coladas. (And gettin\' caught in the rain.)</p></blockquote>\n<!-- /wp:quote -->\n\n<!-- wp:paragraph -->\n<p>...or something like this:</p>\n<!-- /wp:paragraph -->\n\n<!-- wp:quote -->\n<blockquote class=\"wp-block-quote\"><p>The XYZ Doohickey Company was founded in 1971, and has been providing quality doohickeys to the public ever since. Located in Gotham City, XYZ employs over 2,000 people and does all kinds of awesome things for the Gotham community.</p></blockquote>\n<!-- /wp:quote -->\n\n<!-- wp:paragraph -->\n<p>As a new WordPress user, you should go to <a href=\"https://banjaraclothing.com/wp-admin/\">your dashboard</a> to delete this page and create new pages for your content. Have fun!</p>\n<!-- /wp:paragraph -->', 'Sample Page', '', 'publish', 'closed', 'open', '', 'sample-page', '', '', '2021-04-14 18:38:27', '2021-04-14 18:38:27', '', 0, 'https://banjaraclothing.com/?page_id=2', 0, 'page', '', 0),
(3, 1, '2021-04-14 18:38:27', '2021-04-14 18:38:27', '<!-- wp:heading --><h2>Who we are</h2><!-- /wp:heading --><!-- wp:paragraph --><p><strong class=\"privacy-policy-tutorial\">Suggested text: </strong>Our website address is: https://banjaraclothing.com.</p><!-- /wp:paragraph --><!-- wp:heading --><h2>Comments</h2><!-- /wp:heading --><!-- wp:paragraph --><p><strong class=\"privacy-policy-tutorial\">Suggested text: </strong>When visitors leave comments on the site we collect the data shown in the comments form, and also the visitor&#8217;s IP address and browser user agent string to help spam detection.</p><!-- /wp:paragraph --><!-- wp:paragraph --><p>An anonymized string created from your email address (also called a hash) may be provided to the Gravatar service to see if you are using it. The Gravatar service privacy policy is available here: https://automattic.com/privacy/. After approval of your comment, your profile picture is visible to the public in the context of your comment.</p><!-- /wp:paragraph --><!-- wp:heading --><h2>Media</h2><!-- /wp:heading --><!-- wp:paragraph --><p><strong class=\"privacy-policy-tutorial\">Suggested text: </strong>If you upload images to the website, you should avoid uploading images with embedded location data (EXIF GPS) included. Visitors to the website can download and extract any location data from images on the website.</p><!-- /wp:paragraph --><!-- wp:heading --><h2>Cookies</h2><!-- /wp:heading --><!-- wp:paragraph --><p><strong class=\"privacy-policy-tutorial\">Suggested text: </strong>If you leave a comment on our site you may opt-in to saving your name, email address and website in cookies. These are for your convenience so that you do not have to fill in your details again when you leave another comment. These cookies will last for one year.</p><!-- /wp:paragraph --><!-- wp:paragraph --><p>If you visit our login page, we will set a temporary cookie to determine if your browser accepts cookies. This cookie contains no personal data and is discarded when you close your browser.</p><!-- /wp:paragraph --><!-- wp:paragraph --><p>When you log in, we will also set up several cookies to save your login information and your screen display choices. Login cookies last for two days, and screen options cookies last for a year. If you select &quot;Remember Me&quot;, your login will persist for two weeks. If you log out of your account, the login cookies will be removed.</p><!-- /wp:paragraph --><!-- wp:paragraph --><p>If you edit or publish an article, an additional cookie will be saved in your browser. This cookie includes no personal data and simply indicates the post ID of the article you just edited. It expires after 1 day.</p><!-- /wp:paragraph --><!-- wp:heading --><h2>Embedded content from other websites</h2><!-- /wp:heading --><!-- wp:paragraph --><p><strong class=\"privacy-policy-tutorial\">Suggested text: </strong>Articles on this site may include embedded content (e.g. videos, images, articles, etc.). Embedded content from other websites behaves in the exact same way as if the visitor has visited the other website.</p><!-- /wp:paragraph --><!-- wp:paragraph --><p>These websites may collect data about you, use cookies, embed additional third-party tracking, and monitor your interaction with that embedded content, including tracking your interaction with the embedded content if you have an account and are logged in to that website.</p><!-- /wp:paragraph --><!-- wp:heading --><h2>Who we share your data with</h2><!-- /wp:heading --><!-- wp:paragraph --><p><strong class=\"privacy-policy-tutorial\">Suggested text: </strong>If you request a password reset, your IP address will be included in the reset email.</p><!-- /wp:paragraph --><!-- wp:heading --><h2>How long we retain your data</h2><!-- /wp:heading --><!-- wp:paragraph --><p><strong class=\"privacy-policy-tutorial\">Suggested text: </strong>If you leave a comment, the comment and its metadata are retained indefinitely. This is so we can recognize and approve any follow-up comments automatically instead of holding them in a moderation queue.</p><!-- /wp:paragraph --><!-- wp:paragraph --><p>For users that register on our website (if any), we also store the personal information they provide in their user profile. All users can see, edit, or delete their personal information at any time (except they cannot change their username). Website administrators can also see and edit that information.</p><!-- /wp:paragraph --><!-- wp:heading --><h2>What rights you have over your data</h2><!-- /wp:heading --><!-- wp:paragraph --><p><strong class=\"privacy-policy-tutorial\">Suggested text: </strong>If you have an account on this site, or have left comments, you can request to receive an exported file of the personal data we hold about you, including any data you have provided to us. You can also request that we erase any personal data we hold about you. This does not include any data we are obliged to keep for administrative, legal, or security purposes.</p><!-- /wp:paragraph --><!-- wp:heading --><h2>Where we send your data</h2><!-- /wp:heading --><!-- wp:paragraph --><p><strong class=\"privacy-policy-tutorial\">Suggested text: </strong>Visitor comments may be checked through an automated spam detection service.</p><!-- /wp:paragraph -->', 'Privacy Policy', '', 'draft', 'closed', 'open', '', 'privacy-policy', '', '', '2021-04-14 18:38:27', '2021-04-14 18:38:27', '', 0, 'https://banjaraclothing.com/?page_id=3', 0, 'page', '', 0);

-- --------------------------------------------------------

--
-- Table structure for table `wp_termmeta`
--

CREATE TABLE `wp_termmeta` (
  `meta_id` bigint(20) UNSIGNED NOT NULL,
  `term_id` bigint(20) UNSIGNED NOT NULL DEFAULT 0,
  `meta_key` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `meta_value` longtext COLLATE utf8mb4_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Table structure for table `wp_terms`
--

CREATE TABLE `wp_terms` (
  `term_id` bigint(20) UNSIGNED NOT NULL,
  `name` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `slug` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `term_group` bigint(10) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `wp_terms`
--

INSERT INTO `wp_terms` (`term_id`, `name`, `slug`, `term_group`) VALUES
(1, 'Uncategorized', 'uncategorized', 0);

-- --------------------------------------------------------

--
-- Table structure for table `wp_term_relationships`
--

CREATE TABLE `wp_term_relationships` (
  `object_id` bigint(20) UNSIGNED NOT NULL DEFAULT 0,
  `term_taxonomy_id` bigint(20) UNSIGNED NOT NULL DEFAULT 0,
  `term_order` int(11) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `wp_term_relationships`
--

INSERT INTO `wp_term_relationships` (`object_id`, `term_taxonomy_id`, `term_order`) VALUES
(1, 1, 0);

-- --------------------------------------------------------

--
-- Table structure for table `wp_term_taxonomy`
--

CREATE TABLE `wp_term_taxonomy` (
  `term_taxonomy_id` bigint(20) UNSIGNED NOT NULL,
  `term_id` bigint(20) UNSIGNED NOT NULL DEFAULT 0,
  `taxonomy` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `description` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
  `parent` bigint(20) UNSIGNED NOT NULL DEFAULT 0,
  `count` bigint(20) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `wp_term_taxonomy`
--

INSERT INTO `wp_term_taxonomy` (`term_taxonomy_id`, `term_id`, `taxonomy`, `description`, `parent`, `count`) VALUES
(1, 1, 'category', '', 0, 1);

-- --------------------------------------------------------

--
-- Table structure for table `wp_usermeta`
--

CREATE TABLE `wp_usermeta` (
  `umeta_id` bigint(20) UNSIGNED NOT NULL,
  `user_id` bigint(20) UNSIGNED NOT NULL DEFAULT 0,
  `meta_key` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `meta_value` longtext COLLATE utf8mb4_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `wp_usermeta`
--

INSERT INTO `wp_usermeta` (`umeta_id`, `user_id`, `meta_key`, `meta_value`) VALUES
(1, 1, 'nickname', 'hingerharsh'),
(2, 1, 'first_name', ''),
(3, 1, 'last_name', ''),
(4, 1, 'description', ''),
(5, 1, 'rich_editing', 'true'),
(6, 1, 'syntax_highlighting', 'true'),
(7, 1, 'comment_shortcuts', 'false'),
(8, 1, 'admin_color', 'fresh'),
(9, 1, 'use_ssl', '0'),
(10, 1, 'show_admin_bar_front', 'true'),
(11, 1, 'locale', ''),
(12, 1, 'wp_capabilities', 'a:1:{s:13:\"administrator\";b:1;}'),
(13, 1, 'wp_user_level', '10'),
(14, 1, 'dismissed_wp_pointers', ''),
(15, 1, 'show_welcome_panel', '1'),
(16, 1, 'session_tokens', 'a:1:{s:64:\"10e9e57a224c8ad3c00ecfc729da69b98573096a5d7a1a9f709a013dd60cb512\";a:4:{s:10:\"expiration\";i:1618598473;s:2:\"ip\";s:14:\"117.205.86.212\";s:2:\"ua\";s:121:\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36\";s:5:\"login\";i:1618425673;}}'),
(17, 1, 'wp_dashboard_quick_press_last_post_id', '4'),
(18, 1, 'community-events-location', 'a:1:{s:2:\"ip\";s:12:\"117.205.86.0\";}');

-- --------------------------------------------------------

--
-- Table structure for table `wp_users`
--

CREATE TABLE `wp_users` (
  `ID` bigint(20) UNSIGNED NOT NULL,
  `user_login` varchar(60) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `user_pass` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `user_nicename` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `user_email` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `user_url` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `user_registered` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `user_activation_key` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `user_status` int(11) NOT NULL DEFAULT 0,
  `display_name` varchar(250) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `wp_users`
--

INSERT INTO `wp_users` (`ID`, `user_login`, `user_pass`, `user_nicename`, `user_email`, `user_url`, `user_registered`, `user_activation_key`, `user_status`, `display_name`) VALUES
(1, 'hingerharsh', '$P$BMtEDXi/E2MNu4bjysfygVaOLAF0D11', 'hingerharsh', 'hingerharsh@gmail.com', 'https://banjaraclothing.com', '2021-04-14 18:38:27', '', 0, 'hingerharsh');

-- --------------------------------------------------------

--
-- Table structure for table `wp_wpforms_tasks_meta`
--

CREATE TABLE `wp_wpforms_tasks_meta` (
  `id` bigint(20) NOT NULL,
  `action` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `data` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
  `date` datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `wp_wpforms_tasks_meta`
--

INSERT INTO `wp_wpforms_tasks_meta` (`id`, `action`, `data`, `date`) VALUES
(1, 'wpforms_process_entry_emails_meta_cleanup', 'Wzg2NDAwXQ==', '2021-04-14 18:41:14'),
(2, 'wpforms_admin_notifications_update', 'W10=', '2021-04-14 18:49:06'),
(3, 'wpforms_admin_addons_cache_update', 'W10=', '2021-04-14 18:49:07');

--
-- Indexes for dumped tables
--

--
-- Indexes for table `wp_actionscheduler_actions`
--
ALTER TABLE `wp_actionscheduler_actions`
  ADD PRIMARY KEY (`action_id`),
  ADD KEY `hook` (`hook`),
  ADD KEY `status` (`status`),
  ADD KEY `scheduled_date_gmt` (`scheduled_date_gmt`),
  ADD KEY `args` (`args`),
  ADD KEY `group_id` (`group_id`),
  ADD KEY `last_attempt_gmt` (`last_attempt_gmt`),
  ADD KEY `claim_id` (`claim_id`);

--
-- Indexes for table `wp_actionscheduler_claims`
--
ALTER TABLE `wp_actionscheduler_claims`
  ADD PRIMARY KEY (`claim_id`),
  ADD KEY `date_created_gmt` (`date_created_gmt`);

--
-- Indexes for table `wp_actionscheduler_groups`
--
ALTER TABLE `wp_actionscheduler_groups`
  ADD PRIMARY KEY (`group_id`),
  ADD KEY `slug` (`slug`(191));

--
-- Indexes for table `wp_actionscheduler_logs`
--
ALTER TABLE `wp_actionscheduler_logs`
  ADD PRIMARY KEY (`log_id`),
  ADD KEY `action_id` (`action_id`),
  ADD KEY `log_date_gmt` (`log_date_gmt`);

--
-- Indexes for table `wp_aioseo_notifications`
--
ALTER TABLE `wp_aioseo_notifications`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `ndx_aioseo_notifications_slug` (`slug`),
  ADD KEY `ndx_aioseo_notifications_dates` (`start`,`end`),
  ADD KEY `ndx_aioseo_notifications_type` (`type`),
  ADD KEY `ndx_aioseo_notifications_dismissed` (`dismissed`);

--
-- Indexes for table `wp_aioseo_posts`
--
ALTER TABLE `wp_aioseo_posts`
  ADD PRIMARY KEY (`id`),
  ADD KEY `ndx_aioseo_posts_post_id` (`post_id`);

--
-- Indexes for table `wp_commentmeta`
--
ALTER TABLE `wp_commentmeta`
  ADD PRIMARY KEY (`meta_id`),
  ADD KEY `comment_id` (`comment_id`),
  ADD KEY `meta_key` (`meta_key`(191));

--
-- Indexes for table `wp_comments`
--
ALTER TABLE `wp_comments`
  ADD PRIMARY KEY (`comment_ID`),
  ADD KEY `comment_post_ID` (`comment_post_ID`),
  ADD KEY `comment_approved_date_gmt` (`comment_approved`,`comment_date_gmt`),
  ADD KEY `comment_date_gmt` (`comment_date_gmt`),
  ADD KEY `comment_parent` (`comment_parent`),
  ADD KEY `comment_author_email` (`comment_author_email`(10));

--
-- Indexes for table `wp_links`
--
ALTER TABLE `wp_links`
  ADD PRIMARY KEY (`link_id`),
  ADD KEY `link_visible` (`link_visible`);

--
-- Indexes for table `wp_options`
--
ALTER TABLE `wp_options`
  ADD PRIMARY KEY (`option_id`),
  ADD UNIQUE KEY `option_name` (`option_name`),
  ADD KEY `autoload` (`autoload`);

--
-- Indexes for table `wp_postmeta`
--
ALTER TABLE `wp_postmeta`
  ADD PRIMARY KEY (`meta_id`),
  ADD KEY `post_id` (`post_id`),
  ADD KEY `meta_key` (`meta_key`(191));

--
-- Indexes for table `wp_posts`
--
ALTER TABLE `wp_posts`
  ADD PRIMARY KEY (`ID`),
  ADD KEY `post_name` (`post_name`(191)),
  ADD KEY `type_status_date` (`post_type`,`post_status`,`post_date`,`ID`),
  ADD KEY `post_parent` (`post_parent`),
  ADD KEY `post_author` (`post_author`);

--
-- Indexes for table `wp_termmeta`
--
ALTER TABLE `wp_termmeta`
  ADD PRIMARY KEY (`meta_id`),
  ADD KEY `term_id` (`term_id`),
  ADD KEY `meta_key` (`meta_key`(191));

--
-- Indexes for table `wp_terms`
--
ALTER TABLE `wp_terms`
  ADD PRIMARY KEY (`term_id`),
  ADD KEY `slug` (`slug`(191)),
  ADD KEY `name` (`name`(191));

--
-- Indexes for table `wp_term_relationships`
--
ALTER TABLE `wp_term_relationships`
  ADD PRIMARY KEY (`object_id`,`term_taxonomy_id`),
  ADD KEY `term_taxonomy_id` (`term_taxonomy_id`);

--
-- Indexes for table `wp_term_taxonomy`
--
ALTER TABLE `wp_term_taxonomy`
  ADD PRIMARY KEY (`term_taxonomy_id`),
  ADD UNIQUE KEY `term_id_taxonomy` (`term_id`,`taxonomy`),
  ADD KEY `taxonomy` (`taxonomy`);

--
-- Indexes for table `wp_usermeta`
--
ALTER TABLE `wp_usermeta`
  ADD PRIMARY KEY (`umeta_id`),
  ADD KEY `user_id` (`user_id`),
  ADD KEY `meta_key` (`meta_key`(191));

--
-- Indexes for table `wp_users`
--
ALTER TABLE `wp_users`
  ADD PRIMARY KEY (`ID`),
  ADD KEY `user_login_key` (`user_login`),
  ADD KEY `user_nicename` (`user_nicename`),
  ADD KEY `user_email` (`user_email`);

--
-- Indexes for table `wp_wpforms_tasks_meta`
--
ALTER TABLE `wp_wpforms_tasks_meta`
  ADD PRIMARY KEY (`id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `wp_actionscheduler_actions`
--
ALTER TABLE `wp_actionscheduler_actions`
  MODIFY `action_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=334;

--
-- AUTO_INCREMENT for table `wp_actionscheduler_claims`
--
ALTER TABLE `wp_actionscheduler_claims`
  MODIFY `claim_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=658;

--
-- AUTO_INCREMENT for table `wp_actionscheduler_groups`
--
ALTER TABLE `wp_actionscheduler_groups`
  MODIFY `group_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=4;

--
-- AUTO_INCREMENT for table `wp_actionscheduler_logs`
--
ALTER TABLE `wp_actionscheduler_logs`
  MODIFY `log_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=974;

--
-- AUTO_INCREMENT for table `wp_aioseo_notifications`
--
ALTER TABLE `wp_aioseo_notifications`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `wp_aioseo_posts`
--
ALTER TABLE `wp_aioseo_posts`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=3;

--
-- AUTO_INCREMENT for table `wp_commentmeta`
--
ALTER TABLE `wp_commentmeta`
  MODIFY `meta_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `wp_comments`
--
ALTER TABLE `wp_comments`
  MODIFY `comment_ID` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;

--
-- AUTO_INCREMENT for table `wp_links`
--
ALTER TABLE `wp_links`
  MODIFY `link_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `wp_options`
--
ALTER TABLE `wp_options`
  MODIFY `option_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=955;

--
-- AUTO_INCREMENT for table `wp_postmeta`
--
ALTER TABLE `wp_postmeta`
  MODIFY `meta_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=11;

--
-- AUTO_INCREMENT for table `wp_posts`
--
ALTER TABLE `wp_posts`
  MODIFY `ID` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=10;

--
-- AUTO_INCREMENT for table `wp_termmeta`
--
ALTER TABLE `wp_termmeta`
  MODIFY `meta_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `wp_terms`
--
ALTER TABLE `wp_terms`
  MODIFY `term_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;

--
-- AUTO_INCREMENT for table `wp_term_taxonomy`
--
ALTER TABLE `wp_term_taxonomy`
  MODIFY `term_taxonomy_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;

--
-- AUTO_INCREMENT for table `wp_usermeta`
--
ALTER TABLE `wp_usermeta`
  MODIFY `umeta_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=19;

--
-- AUTO_INCREMENT for table `wp_users`
--
ALTER TABLE `wp_users`
  MODIFY `ID` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;

--
-- AUTO_INCREMENT for table `wp_wpforms_tasks_meta`
--
ALTER TABLE `wp_wpforms_tasks_meta`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=4;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
