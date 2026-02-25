<?php
defined('ABSPATH') || exit;

$is_in_cart = false;
if (WC()->cart) {
    foreach (WC()->cart->get_cart() as $cart_item) {
        if (!empty($cart_item['tcche_order_bump_id']) && $cart_item['tcche_order_bump_id'] == $bump['id']) {
            $is_in_cart = true;
            break;
        }
    }
}
?>
<?php $style_class = isset($design_style) ? 'tcche-ob-offer--' . esc_attr($design_style) : ''; ?>
<div class="tcche-ob-offer <?php echo $style_class; ?> <?php echo $is_in_cart ? 'tcche-ob-offer--accepted' : ''; ?>"
     data-bump-id="<?php echo esc_attr($bump['id']); ?>">

    <div class="tcche-ob-offer__header">
        <label class="tcche-ob-offer__checkbox-label">
            <input type="checkbox"
                   class="tcche-ob-offer__checkbox"
                   <?php checked($is_in_cart); ?>
                   data-bump-id="<?php echo esc_attr($bump['id']); ?>">
            <span class="tcche-ob-offer__checkmark"></span>
            <span class="tcche-ob-offer__headline">
                <?php echo nl2br(esc_html($bump['headline'])); ?>
            </span>
        </label>
    </div>

    <div class="tcche-ob-offer__body">
        <?php if ($bump['bump_product_image']) : ?>
            <div class="tcche-ob-offer__image">
                <img src="<?php echo esc_url($bump['bump_product_image']); ?>"
                     alt="<?php echo esc_attr($product->get_name()); ?>">
            </div>
        <?php endif; ?>

        <div class="tcche-ob-offer__content">
            <h4 class="tcche-ob-offer__product-name">
                <?php echo esc_html($product->get_name()); ?>
            </h4>

            <?php if ($bump['description']) : ?>
                <div class="tcche-ob-offer__description">
                    <?php echo wp_kses_post(nl2br($bump['description'])); ?>
                </div>
            <?php endif; ?>

            <div class="tcche-ob-offer__price">
                <?php if ($has_discount) : ?>
                    <span class="tcche-ob-offer__price-original">
                        <?php echo wc_price($original_price); ?>
                    </span>
                    <span class="tcche-ob-offer__price-bump">
                        <?php echo wc_price($bump_price); ?>
                    </span>
                    <span class="tcche-ob-offer__discount-badge">
                        <?php
                        if ($bump['discount_type'] === 'percentage') {
                            printf(__('Save %s%%', 'tcche-order-bump'), $bump['discount_value']);
                        } else {
                            printf(__('Save %s', 'tcche-order-bump'), wc_price($bump['discount_value']));
                        }
                        ?>
                    </span>
                <?php else : ?>
                    <span class="tcche-ob-offer__price-bump">
                        <?php echo wc_price($bump_price); ?>
                    </span>
                <?php endif; ?>
            </div>
        </div>
    </div>

    <div class="tcche-ob-offer__loading" style="display:none;">
        <span class="tcche-ob-offer__spinner"></span>
    </div>
</div>
