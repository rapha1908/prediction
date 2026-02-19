<?php
/**
 * Plugin Name: TCCHE Order Bump
 * Plugin URI:  https://tcche.com
 * Description: Order Bump system for WooCommerce with analytics dashboard and REST API.
 * Version:     1.0.0
 * Author:      TCCHE
 * Author URI:  https://tcche.com
 * Text Domain: tcche-order-bump
 * Domain Path: /languages
 * Requires at least: 6.0
 * Requires PHP: 7.4
 * WC requires at least: 7.0
 */

defined('ABSPATH') || exit;

define('TCCHE_OB_VERSION', '1.0.1');
define('TCCHE_OB_PLUGIN_DIR', plugin_dir_path(__FILE__));
define('TCCHE_OB_PLUGIN_URL', plugin_dir_url(__FILE__));
define('TCCHE_OB_PLUGIN_BASENAME', plugin_basename(__FILE__));

final class TCCHE_Order_Bump {

    private static $instance = null;

    public static function instance() {
        if (is_null(self::$instance)) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    private function __construct() {
        $this->check_dependencies();
        $this->includes();
        $this->init_hooks();
    }

    private function check_dependencies() {
        add_action('admin_init', function () {
            if (!class_exists('WooCommerce')) {
                add_action('admin_notices', function () {
                    echo '<div class="error"><p><strong>TCCHE Order Bump</strong> requires WooCommerce to be installed and active.</p></div>';
                });
                deactivate_plugins(TCCHE_OB_PLUGIN_BASENAME);
            }
        });
    }

    private function includes() {
        require_once TCCHE_OB_PLUGIN_DIR . 'includes/class-tcche-ob-post-type.php';
        require_once TCCHE_OB_PLUGIN_DIR . 'includes/class-tcche-ob-admin.php';
        require_once TCCHE_OB_PLUGIN_DIR . 'includes/class-tcche-ob-checkout.php';
        require_once TCCHE_OB_PLUGIN_DIR . 'includes/class-tcche-ob-analytics.php';
        require_once TCCHE_OB_PLUGIN_DIR . 'includes/class-tcche-ob-rest-api.php';
        require_once TCCHE_OB_PLUGIN_DIR . 'includes/class-tcche-ob-ajax.php';
    }

    private function init_hooks() {
        register_activation_hook(__FILE__, [$this, 'activate']);
        register_deactivation_hook(__FILE__, [$this, 'deactivate']);

        add_action('init', [TCCHE_OB_Post_Type::class, 'register']);
        add_action('init', [$this, 'maybe_create_tables']);
        add_action('init', [$this, 'ensure_session_cookie']);
        add_action('admin_menu', [TCCHE_OB_Admin::class, 'register_menus']);
        add_action('admin_enqueue_scripts', [TCCHE_OB_Admin::class, 'enqueue_assets']);

        if (!is_admin()) {
            add_action('wp_enqueue_scripts', [TCCHE_OB_Checkout::class, 'enqueue_assets']);
        }

        TCCHE_OB_Checkout::init();
        TCCHE_OB_Analytics::init();
        TCCHE_OB_Ajax::init();

        add_action('rest_api_init', [TCCHE_OB_REST_API::class, 'register_routes']);
    }

    public function maybe_create_tables() {
        $installed_version = get_option('tcche_ob_db_version', '0');
        if (version_compare($installed_version, TCCHE_OB_VERSION, '<')) {
            TCCHE_OB_Analytics::create_tables();
            update_option('tcche_ob_db_version', TCCHE_OB_VERSION);
        }
    }

    public function ensure_session_cookie() {
        if (is_admin() || wp_doing_cron() || defined('REST_REQUEST')) {
            return;
        }
        if (empty($_COOKIE['tcche_ob_sid'])) {
            $sid = 'ob_' . wp_generate_uuid4();
            setcookie('tcche_ob_sid', $sid, time() + 3600, COOKIEPATH, COOKIE_DOMAIN, is_ssl(), false);
            $_COOKIE['tcche_ob_sid'] = $sid;
        }
    }

    public function activate() {
        TCCHE_OB_Analytics::create_tables();
        update_option('tcche_ob_db_version', TCCHE_OB_VERSION);
        TCCHE_OB_Post_Type::register();
        flush_rewrite_rules();
    }

    public function deactivate() {
        flush_rewrite_rules();
    }
}

add_action('plugins_loaded', function () {
    TCCHE_Order_Bump::instance();
});
