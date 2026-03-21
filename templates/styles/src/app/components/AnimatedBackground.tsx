export function AnimatedBackground() {
  return (
    <div 
      className="fixed inset-0 pointer-events-none z-0"
      style={{
        background: `
          linear-gradient(135deg, 
            rgba(173, 216, 230, 0.15) 0%,
            rgba(144, 238, 144, 0.18) 25%,
            rgba(255, 253, 208, 0.15) 50%,
            rgba(255, 218, 185, 0.12) 75%,
            rgba(173, 216, 230, 0.15) 100%
          )
        `,
        backgroundColor: '#FAFBFC'
      }}
    />
  );
}
