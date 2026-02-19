<?php
defined('ABSPATH') || exit;

class TCCHE_OB_Analytics {

    public static function init() {
        // Analytics hooks are called directly from Checkout and Ajax classes
    }

    public static function create_tables() {
        global $wpdb;
        $charset = $wpdb->get_charset_collate();

        $impressions_table = $wpdb->prefix . 'tcche_ob_impressions';
        $conversions_table = $wpdb->prefix . 'tcche_ob_conversions';

        $sql = "CREATE TABLE IF NOT EXISTS {$impressions_table} (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            bump_id BIGINT UNSIGNED NOT NULL,
            session_id VARCHAR(100) DEFAULT '',
            user_id BIGINT UNSIGNED DEFAULT 0,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            KEY bump_id (bump_id),
            KEY created_at (created_at)
        ) {$charset};

        CREATE TABLE IF NOT EXISTS {$conversions_table} (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            bump_id BIGINT UNSIGNED NOT NULL,
            order_id BIGINT UNSIGNED NOT NULL,
            user_id BIGINT UNSIGNED DEFAULT 0,
            revenue DECIMAL(10,2) NOT NULL DEFAULT 0,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            KEY bump_id (bump_id),
            KEY order_id (order_id),
            KEY created_at (created_at)
        ) {$charset};";

        require_once ABSPATH . 'wp-admin/includes/upgrade.php';
        dbDelta($sql);
    }

    public static function track_impression($bump_id) {
        global $wpdb;

        $session_id = self::get_session_id();
        $user_id    = get_current_user_id();

        // Avoid duplicate impressions per session
        $exists = $wpdb->get_var($wpdb->prepare(
            "SELECT COUNT(*) FROM {$wpdb->prefix}tcche_ob_impressions WHERE bump_id = %d AND session_id = %s AND created_at > %s",
            $bump_id,
            $session_id,
            gmdate('Y-m-d H:i:s', strtotime('-1 hour'))
        ));

        if ($exists) {
            return;
        }

        $wpdb->insert($wpdb->prefix . 'tcche_ob_impressions', [
            'bump_id'    => $bump_id,
            'session_id' => $session_id,
            'user_id'    => $user_id,
            'created_at' => current_time('mysql', true),
        ]);
    }

    public static function track_conversion($bump_id, $order_id, $revenue) {
        global $wpdb;

        $wpdb->insert($wpdb->prefix . 'tcche_ob_conversions', [
            'bump_id'    => $bump_id,
            'order_id'   => $order_id,
            'user_id'    => get_current_user_id(),
            'revenue'    => $revenue,
            'created_at' => current_time('mysql', true),
        ]);
    }

    public static function get_stats($args = []) {
        global $wpdb;

        $defaults = [
            'bump_id'    => 0,
            'date_from'  => gmdate('Y-m-d', strtotime('-30 days')),
            'date_to'    => gmdate('Y-m-d'),
        ];
        $args = wp_parse_args($args, $defaults);

        $imp_table  = $wpdb->prefix . 'tcche_ob_impressions';
        $conv_table = $wpdb->prefix . 'tcche_ob_conversions';

        $where_imp  = "WHERE i.created_at BETWEEN %s AND %s";
        $where_conv = "WHERE c.created_at BETWEEN %s AND %s";
        $params_imp  = [$args['date_from'] . ' 00:00:00', $args['date_to'] . ' 23:59:59'];
        $params_conv = [$args['date_from'] . ' 00:00:00', $args['date_to'] . ' 23:59:59'];

        if ($args['bump_id']) {
            $where_imp  .= " AND i.bump_id = %d";
            $where_conv .= " AND c.bump_id = %d";
            $params_imp[]  = $args['bump_id'];
            $params_conv[] = $args['bump_id'];
        }

        $impressions = (int) $wpdb->get_var($wpdb->prepare(
            "SELECT COUNT(*) FROM {$imp_table} i {$where_imp}",
            ...$params_imp
        ));

        $conv_row = $wpdb->get_row($wpdb->prepare(
            "SELECT COUNT(*) as total, COALESCE(SUM(c.revenue), 0) as revenue FROM {$conv_table} c {$where_conv}",
            ...$params_conv
        ));

        $conversions   = (int) $conv_row->total;
        $total_revenue = (float) $conv_row->revenue;

        return [
            'impressions'     => $impressions,
            'conversions'     => $conversions,
            'conversion_rate' => $impressions > 0 ? round(($conversions / $impressions) * 100, 2) : 0,
            'total_revenue'   => $total_revenue,
            'avg_order_value' => $conversions > 0 ? round($total_revenue / $conversions, 2) : 0,
        ];
    }

    public static function get_stats_by_bump($args = []) {
        global $wpdb;

        $defaults = [
            'date_from' => gmdate('Y-m-d', strtotime('-30 days')),
            'date_to'   => gmdate('Y-m-d'),
        ];
        $args = wp_parse_args($args, $defaults);

        $bumps = TCCHE_OB_Post_Type::get_bumps(['post_status' => 'any']);
        $results = [];

        foreach ($bumps as $bump) {
            $stats = self::get_stats([
                'bump_id'   => $bump['id'],
                'date_from' => $args['date_from'],
                'date_to'   => $args['date_to'],
            ]);

            $results[] = array_merge(['bump' => $bump], $stats);
        }

        usort($results, function ($a, $b) {
            return $b['total_revenue'] <=> $a['total_revenue'];
        });

        return $results;
    }

    public static function get_daily_stats($args = []) {
        global $wpdb;

        $defaults = [
            'bump_id'   => 0,
            'date_from' => gmdate('Y-m-d', strtotime('-30 days')),
            'date_to'   => gmdate('Y-m-d'),
        ];
        $args = wp_parse_args($args, $defaults);

        $imp_table  = $wpdb->prefix . 'tcche_ob_impressions';
        $conv_table = $wpdb->prefix . 'tcche_ob_conversions';

        $bump_filter_imp  = $args['bump_id'] ? $wpdb->prepare("AND bump_id = %d", $args['bump_id']) : '';
        $bump_filter_conv = $args['bump_id'] ? $wpdb->prepare("AND bump_id = %d", $args['bump_id']) : '';

        $impressions_daily = $wpdb->get_results($wpdb->prepare(
            "SELECT DATE(created_at) as date, COUNT(*) as count
             FROM {$imp_table}
             WHERE created_at BETWEEN %s AND %s {$bump_filter_imp}
             GROUP BY DATE(created_at)
             ORDER BY date ASC",
            $args['date_from'] . ' 00:00:00',
            $args['date_to'] . ' 23:59:59'
        ), ARRAY_A);

        $conversions_daily = $wpdb->get_results($wpdb->prepare(
            "SELECT DATE(created_at) as date, COUNT(*) as count, SUM(revenue) as revenue
             FROM {$conv_table}
             WHERE created_at BETWEEN %s AND %s {$bump_filter_conv}
             GROUP BY DATE(created_at)
             ORDER BY date ASC",
            $args['date_from'] . ' 00:00:00',
            $args['date_to'] . ' 23:59:59'
        ), ARRAY_A);

        $imp_map  = wp_list_pluck($impressions_daily, 'count', 'date');
        $conv_map = wp_list_pluck($conversions_daily, 'count', 'date');
        $rev_map  = wp_list_pluck($conversions_daily, 'revenue', 'date');

        $days = [];
        $current = strtotime($args['date_from']);
        $end     = strtotime($args['date_to']);

        while ($current <= $end) {
            $d = gmdate('Y-m-d', $current);
            $days[] = [
                'date'        => $d,
                'impressions' => (int) ($imp_map[$d] ?? 0),
                'conversions' => (int) ($conv_map[$d] ?? 0),
                'revenue'     => (float) ($rev_map[$d] ?? 0),
            ];
            $current = strtotime('+1 day', $current);
        }

        return $days;
    }

    private static function get_session_id() {
        if (!isset($_COOKIE['tcche_ob_sid'])) {
            return wp_generate_uuid4();
        }
        return sanitize_text_field($_COOKIE['tcche_ob_sid']);
    }
}
