/**
 * Card items type mapper
 * @type { [key: string]: string }
 */
const CARD_TYPE_MAP = {
    FILE: 'file',
    YOUTUBE: 'youtube'
};

// entry point
$(async () => await initCards());


/**
 * Fetches cards config and init the widget
 * @returns Promise<void>
 */
async function initCards() {
    const response = await fetch('/js/general/home/home_config.json').then((resp) => resp.json());
    const $container = $('#flex-grid');

    response.collection.forEach((item) => {
        if (item.type === CARD_TYPE_MAP.FILE) {
            $container.append(`
                <a class="flex-grid-item" 
                   target="_blank" href="${item.url}" 
                   style="${item.preview_image ? `background-image: url(${item.preview_image});` : ''}">
                    <div class="flex-grid-item-container">
                        <div class="content">${item.title}</div>
                    </div>
                </a>
            `);
        }

        if (item.type === CARD_TYPE_MAP.YOUTUBE) {
            $container.append(`
                <div class="flex-grid-item">
                    <div class="flex-grid-item-container">
                        <div class="content no-padding">
                            <iframe src="${item.url}" 
                                    title="${item.title}" 
                                    allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" 
                                    allowfullscreen>        
                            </iframe>
                        </div>
                    </div>
                </div>
            `);
        }
    });
}