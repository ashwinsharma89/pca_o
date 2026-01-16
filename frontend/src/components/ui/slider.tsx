"use client"

import * as React from "react"
import { cn } from "@/lib/utils"

const Slider = React.forwardRef<
    HTMLInputElement,
    Omit<React.InputHTMLAttributes<HTMLInputElement>, 'value' | 'onChange'> & {
        value: number[]
        onValueChange: (value: number[]) => void
        max?: number
        min?: number
        step?: number
    }
>(({ className, value, onValueChange, min = 0, max = 100, step = 1, ...props }, ref) => {

    const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        const val = parseFloat(e.target.value);
        onValueChange([val]);
    };

    return (
        <input
            type="range"
            min={min}
            max={max}
            step={step}
            value={value[0]}
            onChange={handleChange}
            className={cn(
                "w-full h-2 bg-secondary rounded-lg appearance-none cursor-pointer accent-primary",
                className
            )}
            ref={ref}
            {...props}
        />
    )
})
Slider.displayName = "Slider"

export { Slider }
